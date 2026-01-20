package com.titankv.cluster;

import com.titankv.TitanKVClient;
import com.titankv.client.ClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

/**
 * Hinted handoff for handling writes to unavailable nodes.
 * When a replica node is down, hints are stored locally and delivered when the node recovers.
 * 
 * Hint format (on disk):
 * - 4 bytes: magic (HINT)
 * - 2 bytes: version
 * - 4 bytes: target node ID length
 * - N bytes: target node ID
 * - 4 bytes: key length
 * - M bytes: key
 * - 4 bytes: value length (-1 for tombstone)
 * - P bytes: value (if length >= 0)
 * - 8 bytes: timestamp
 * - 8 bytes: expiresAt
 * - 8 bytes: created timestamp
 * - 4 bytes: CRC32
 */
public class HintedHandoff {

    private static final Logger logger = LoggerFactory.getLogger(HintedHandoff.class);

    private static final int HINT_MAGIC = 0x48494E54; // "HINT"
    private static final short HINT_VERSION = 1;
    private static final long MAX_HINT_AGE_MS = 3 * 60 * 60 * 1000; // 3 hours
    private static final long MAX_HINTS_SIZE_BYTES = 256 * 1024 * 1024; // 256MB
    private static final long DELIVERY_INTERVAL_MS = 10_000; // 10 seconds
    private static final int MAX_HINTS_PER_DELIVERY = 100;

    private final ClusterManager clusterManager;
    private final Path hintsDir;
    private final Map<String, FileChannel> hintFiles;
    private final Map<String, AtomicLong> hintCounts;
    private final ScheduledExecutorService deliveryExecutor;
    private final ExecutorService workerPool;
    private final Map<String, TitanKVClient> nodeClients;
    private final String authToken;
    private volatile boolean running;
    private final AtomicLong totalHintBytes;

    /**
     * Create a hinted handoff handler.
     * 
     * @param clusterManager the cluster manager
     * @param dataDir        the data directory for storing hints
     */
    public HintedHandoff(ClusterManager clusterManager, Path dataDir) {
        this.clusterManager = clusterManager;
        this.hintsDir = dataDir.resolve("hints");
        this.hintFiles = new ConcurrentHashMap<>();
        this.hintCounts = new ConcurrentHashMap<>();
        this.nodeClients = new ConcurrentHashMap<>();
        this.totalHintBytes = new AtomicLong(0);
        this.authToken = readAuthToken();
        this.running = false;

        this.deliveryExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "hinted-handoff-delivery");
            t.setDaemon(true);
            return t;
        });

        this.workerPool = Executors.newFixedThreadPool(4, r -> {
            Thread t = new Thread(r, "hinted-handoff-worker");
            t.setDaemon(true);
            return t;
        });

        // Register for cluster events
        clusterManager.addEventListener(this::handleClusterEvent);
    }

    /**
     * Start the hinted handoff system.
     */
    public void start() {
        if (running) {
            return;
        }
        running = true;

        try {
            Files.createDirectories(hintsDir);
            loadExistingHints();
        } catch (IOException e) {
            logger.error("Failed to initialize hints directory: {}", e.getMessage());
        }

        // Schedule periodic hint delivery
        deliveryExecutor.scheduleAtFixedRate(
            this::deliverAllHints,
            DELIVERY_INTERVAL_MS,
            DELIVERY_INTERVAL_MS,
            TimeUnit.MILLISECONDS
        );

        logger.info("Hinted handoff started with hints directory: {}", hintsDir);
    }

    /**
     * Stop the hinted handoff system.
     */
    public void stop() {
        if (!running) {
            return;
        }
        running = false;

        deliveryExecutor.shutdown();
        workerPool.shutdown();

        try {
            if (!deliveryExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                deliveryExecutor.shutdownNow();
            }
            if (!workerPool.awaitTermination(5, TimeUnit.SECONDS)) {
                workerPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            deliveryExecutor.shutdownNow();
            workerPool.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Close all hint files
        for (FileChannel channel : hintFiles.values()) {
            try {
                channel.close();
            } catch (IOException e) {
                logger.debug("Error closing hint file: {}", e.getMessage());
            }
        }
        hintFiles.clear();

        // Close all clients
        for (TitanKVClient client : nodeClients.values()) {
            client.close();
        }
        nodeClients.clear();

        logger.info("Hinted handoff stopped");
    }

    /**
     * Store a hint for a node that is currently unavailable.
     * 
     * @param targetNode the target node
     * @param key        the key
     * @param value      the value (null for delete/tombstone)
     * @param timestamp  the timestamp
     * @param expiresAt  the expiration time
     */
    public void storeHint(Node targetNode, String key, byte[] value, long timestamp, long expiresAt) {
        if (!running) {
            logger.warn("Hinted handoff not running, dropping hint for {}", targetNode.getId());
            return;
        }

        if (totalHintBytes.get() >= MAX_HINTS_SIZE_BYTES) {
            logger.warn("Hint storage limit reached ({}MB), dropping hint for {}",
                MAX_HINTS_SIZE_BYTES / (1024 * 1024), targetNode.getId());
            return;
        }

        try {
            byte[] hintData = encodeHint(targetNode.getId(), key, value, timestamp, expiresAt);
            
            FileChannel channel = getHintFile(targetNode.getId());
            synchronized (channel) {
                ByteBuffer buffer = ByteBuffer.wrap(hintData);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                channel.force(false);
            }

            totalHintBytes.addAndGet(hintData.length);
            hintCounts.computeIfAbsent(targetNode.getId(), k -> new AtomicLong(0)).incrementAndGet();

            logger.debug("Stored hint for node {} (key={})", targetNode.getId(), key);
        } catch (IOException e) {
            logger.error("Failed to store hint for node {}: {}", targetNode.getId(), e.getMessage());
        }
    }

    private byte[] encodeHint(String targetNodeId, String key, byte[] value, long timestamp, long expiresAt) {
        byte[] targetBytes = targetNodeId.getBytes(StandardCharsets.UTF_8);
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        int valueLen = value != null ? value.length : -1;
        int valueSize = valueLen >= 0 ? valueLen : 0;

        int size = 4 + 2 + 4 + targetBytes.length + 4 + keyBytes.length + 4 + valueSize + 8 + 8 + 8 + 4;
        ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.putInt(HINT_MAGIC);
        buffer.putShort(HINT_VERSION);
        buffer.putInt(targetBytes.length);
        buffer.put(targetBytes);
        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);
        buffer.putInt(valueLen);
        if (valueLen >= 0) {
            buffer.put(value);
        }
        buffer.putLong(timestamp);
        buffer.putLong(expiresAt);
        buffer.putLong(System.currentTimeMillis()); // created timestamp

        // CRC
        CRC32 crc = new CRC32();
        crc.update(buffer.array(), 0, buffer.position());
        buffer.putInt((int) crc.getValue());

        return buffer.array();
    }

    private FileChannel getHintFile(String nodeId) throws IOException {
        return hintFiles.computeIfAbsent(nodeId, id -> {
            try {
                Path hintFile = hintsDir.resolve(sanitizeFileName(id) + ".hints");
                return FileChannel.open(hintFile,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.APPEND);
            } catch (IOException e) {
                throw new RuntimeException("Failed to open hint file for " + id, e);
            }
        });
    }

    private String sanitizeFileName(String nodeId) {
        return nodeId.replaceAll("[^a-zA-Z0-9._-]", "_");
    }

    private void loadExistingHints() throws IOException {
        if (!Files.exists(hintsDir)) {
            return;
        }

        try (var stream = Files.newDirectoryStream(hintsDir, "*.hints")) {
            for (Path hintFile : stream) {
                long size = Files.size(hintFile);
                totalHintBytes.addAndGet(size);
                
                String fileName = hintFile.getFileName().toString();
                String nodeId = fileName.substring(0, fileName.length() - ".hints".length());
                
                // Count hints (rough estimate based on average hint size)
                long estimatedCount = size / 100; // Assume ~100 bytes per hint on average
                hintCounts.put(nodeId, new AtomicLong(estimatedCount));
                
                logger.info("Found existing hints for node {}: {} bytes", nodeId, size);
            }
        }
    }

    private void handleClusterEvent(ClusterManager.ClusterEvent event) {
        if (event.getType() == ClusterManager.ClusterEvent.Type.NODE_RECOVERED ||
            event.getType() == ClusterManager.ClusterEvent.Type.NODE_JOINED) {
            
            Node node = event.getNode();
            logger.info("Node {} is available, scheduling hint delivery", node.getId());
            
            // Schedule immediate delivery for this node
            workerPool.submit(() -> deliverHintsToNode(node));
        }
    }

    private void deliverAllHints() {
        if (!running) {
            return;
        }

        for (Node node : clusterManager.getAllNodes()) {
            if (!node.equals(clusterManager.getLocalNode()) && node.isAvailable()) {
                AtomicLong count = hintCounts.get(node.getId());
                if (count != null && count.get() > 0) {
                    workerPool.submit(() -> deliverHintsToNode(node));
                }
            }
        }
    }

    private void deliverHintsToNode(Node node) {
        if (!running || !node.isAvailable()) {
            return;
        }

        Path hintFile = hintsDir.resolve(sanitizeFileName(node.getId()) + ".hints");
        if (!Files.exists(hintFile)) {
            return;
        }

        logger.info("Delivering hints to node {}", node.getId());

        try {
            TitanKVClient client = getClient(node);
            int delivered = 0;
            int failed = 0;
            long now = System.currentTimeMillis();

            try (FileChannel channel = FileChannel.open(hintFile, StandardOpenOption.READ)) {
                while (delivered < MAX_HINTS_PER_DELIVERY && channel.position() < channel.size()) {
                    try {
                        HintRecord hint = readHint(channel);
                        if (hint == null) {
                            break; // Corrupted or incomplete hint
                        }

                        // Skip expired hints
                        if (now - hint.createdAt > MAX_HINT_AGE_MS) {
                            logger.debug("Skipping expired hint for key {}", hint.key);
                            continue;
                        }

                        // Deliver the hint
                        if (hint.value != null) {
                            client.putInternal(hint.key, hint.value, hint.timestamp, hint.expiresAt);
                        } else {
                            client.deleteInternal(hint.key, hint.timestamp, hint.expiresAt);
                        }
                        delivered++;

                    } catch (IOException e) {
                        logger.warn("Failed to deliver hint to {}: {}", node.getId(), e.getMessage());
                        failed++;
                        if (failed >= 3) {
                            break; // Node might be failing again
                        }
                    }
                }
            }

            // If all hints delivered, remove the hint file
            if (delivered > 0 && failed == 0) {
                try {
                    // Close and remove the hint file channel
                    FileChannel oldChannel = hintFiles.remove(node.getId());
                    if (oldChannel != null) {
                        oldChannel.close();
                    }
                    
                    long fileSize = Files.size(hintFile);
                    Files.delete(hintFile);
                    totalHintBytes.addAndGet(-fileSize);
                    hintCounts.remove(node.getId());
                    
                    logger.info("Delivered {} hints to node {} and cleared hint file", delivered, node.getId());
                } catch (IOException e) {
                    logger.warn("Failed to remove hint file for {}: {}", node.getId(), e.getMessage());
                }
            } else {
                logger.info("Delivered {} hints to node {} ({} failed)", delivered, node.getId(), failed);
            }

        } catch (Exception e) {
            logger.error("Error delivering hints to node {}: {}", node.getId(), e.getMessage());
        }
    }

    private HintRecord readHint(FileChannel channel) throws IOException {
        ByteBuffer header = ByteBuffer.allocate(4 + 2 + 4);
        if (readFully(channel, header) < header.capacity()) {
            return null;
        }
        header.flip();

        int magic = header.getInt();
        short version = header.getShort();
        int targetLen = header.getInt();

        if (magic != HINT_MAGIC || version != HINT_VERSION) {
            logger.warn("Invalid hint header, skipping");
            return null;
        }

        // Read and discard target node ID (we already know it from the file name)
        ByteBuffer targetBuf = ByteBuffer.allocate(targetLen);
        if (readFully(channel, targetBuf) < targetLen) {
            return null;
        }
        // targetNodeId is stored in file for completeness but not needed here

        ByteBuffer keyLenBuf = ByteBuffer.allocate(4);
        if (readFully(channel, keyLenBuf) < 4) {
            return null;
        }
        keyLenBuf.flip();
        int keyLen = keyLenBuf.getInt();

        ByteBuffer keyBuf = ByteBuffer.allocate(keyLen);
        if (readFully(channel, keyBuf) < keyLen) {
            return null;
        }
        keyBuf.flip();
        String key = new String(keyBuf.array(), StandardCharsets.UTF_8);

        ByteBuffer valueLenBuf = ByteBuffer.allocate(4);
        if (readFully(channel, valueLenBuf) < 4) {
            return null;
        }
        valueLenBuf.flip();
        int valueLen = valueLenBuf.getInt();

        byte[] value = null;
        if (valueLen >= 0) {
            ByteBuffer valueBuf = ByteBuffer.allocate(valueLen);
            if (readFully(channel, valueBuf) < valueLen) {
                return null;
            }
            valueBuf.flip();
            value = valueBuf.array();
        }

        ByteBuffer metaBuf = ByteBuffer.allocate(8 + 8 + 8 + 4);
        if (readFully(channel, metaBuf) < metaBuf.capacity()) {
            return null;
        }
        metaBuf.flip();

        long timestamp = metaBuf.getLong();
        long expiresAt = metaBuf.getLong();
        long createdAt = metaBuf.getLong();
        metaBuf.getInt(); // CRC - verification can be added for production

        return new HintRecord(key, value, timestamp, expiresAt, createdAt);
    }

    private int readFully(FileChannel channel, ByteBuffer buffer) throws IOException {
        int total = 0;
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer);
            if (read == -1) {
                return total;
            }
            total += read;
        }
        return total;
    }

    private TitanKVClient getClient(Node node) {
        return nodeClients.computeIfAbsent(node.getAddress(), addr -> {
            ClientConfig config = ClientConfig.builder()
                .connectTimeoutMs(5000)
                .readTimeoutMs(5000)
                .retryOnFailure(false)
                .authToken(authToken)
                .build();
            return new TitanKVClient(config, addr);
        });
    }

    private static String readAuthToken() {
        String value = System.getenv("TITANKV_INTERNAL_TOKEN");
        if (value == null || value.isEmpty()) {
            value = System.getProperty("titankv.internal.token");
        }
        if (value == null || value.isEmpty()) {
            value = System.getenv("TITANKV_CLUSTER_SECRET");
        }
        if (value == null || value.isEmpty()) {
            value = System.getProperty("titankv.cluster.secret");
        }
        return value;
    }

    /**
     * Get the number of pending hints for a node.
     */
    public long getPendingHintCount(String nodeId) {
        AtomicLong count = hintCounts.get(nodeId);
        return count != null ? count.get() : 0;
    }

    /**
     * Get total hint storage size in bytes.
     */
    public long getTotalHintBytes() {
        return totalHintBytes.get();
    }

    private static class HintRecord {
        final String key;
        final byte[] value;
        final long timestamp;
        final long expiresAt;
        final long createdAt;

        HintRecord(String key, byte[] value, long timestamp, long expiresAt, long createdAt) {
            this.key = key;
            this.value = value;
            this.timestamp = timestamp;
            this.expiresAt = expiresAt;
            this.createdAt = createdAt;
        }
    }
}
