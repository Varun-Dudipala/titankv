package com.titankv.consistency;

import com.titankv.cluster.ClusterManager;
import com.titankv.cluster.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.zip.CRC32;

/**
 * Hinted handoff: when a replica is down or a write to it fails, the coordinator keeps the write
 * as a hint and delivers it once the replica is back, so a returning node catches up on writes it
 * missed without waiting for reads or anti-entropy to find them.
 *
 * Hints are kept per target node and coalesced per key: only the newest version matters under
 * last-write-wins. When a hints directory is given they are also appended to
 * {@code <dir>/<node>.hints} (CRC-checked records) and reloaded on startup; a node's file is
 * truncated once all its hints are delivered, and rewritten from memory when overwrites of the same
 * keys have made it much larger than the pending hints. Hints are not stored for a node that has
 * been down longer than the hint window; anti-entropy repair covers those. Hints for a node that
 * is removed from the cluster are dropped.
 */
public final class HintedHandoff {

    private static final Logger logger = LoggerFactory.getLogger(HintedHandoff.class);

    static final long HINT_WINDOW_MS = 3 * 60 * 60 * 1000L;
    static final int MAX_HINTS_PER_NODE = 100_000;
    private static final long DELIVERY_INTERVAL_MS = 2_000;
    // A hint file is rewritten once it holds this many times more records than pending hints
    private static final int COMPACTION_RATIO = 4;
    private static final int MIN_RECORDS_BEFORE_COMPACTION = 10_000;

    private record Hint(String key, byte[] value, long timestamp, long expiresAt) {
    }

    private final ClusterManager clusterManager;
    private final ReplicaIO replicaIO;
    private final Path hintsDir;
    private final Map<String, ConcurrentHashMap<String, Hint>> pending = new ConcurrentHashMap<>();
    private final Map<String, Object> locks = new ConcurrentHashMap<>();
    private final Map<String, FileChannel> files = new ConcurrentHashMap<>();
    private final Map<String, Integer> recordsInFile = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler;
    private final Set<String> delivering = ConcurrentHashMap.newKeySet();
    private final java.util.concurrent.atomic.AtomicLong droppedHints = new java.util.concurrent.atomic.AtomicLong();

    /**
     * @param hintsDir directory to persist hints in, or null to keep them in memory only
     */
    public HintedHandoff(ClusterManager clusterManager, ReplicaIO replicaIO, Path hintsDir) {
        this.clusterManager = clusterManager;
        this.replicaIO = replicaIO;
        this.hintsDir = hintsDir;
        if (hintsDir != null) {
            loadPersistedHints();
        }
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "hinted-handoff");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(this::deliverAll, DELIVERY_INTERVAL_MS, DELIVERY_INTERVAL_MS,
                TimeUnit.MILLISECONDS);
        clusterManager.addEventListener(event -> {
            switch (event.getType()) {
                case NODE_RECOVERED:
                case NODE_JOINED:
                case NODE_RESTARTED:
                    scheduler.execute(() -> deliver(event.getNode()));
                    break;
                case NODE_LEFT:
                    // Removed for good: its keys have new replicas, which anti-entropy fills
                    scheduler.execute(() -> discard(event.getNode().getId()));
                    break;
                default:
                    break;
            }
        });
    }

    /**
     * Keep a write for a replica that did not receive it.
     *
     * @param value the value, or null for a delete (tombstone)
     */
    public void store(Node target, String key, byte[] value, long timestamp, long expiresAt) {
        if (target.getMillisSinceLastHeartbeat() > HINT_WINDOW_MS) {
            logger.debug("Not storing hint for {}: down longer than the hint window", target.getId());
            return;
        }
        Hint hint = new Hint(key, value, timestamp, expiresAt);
        synchronized (lockFor(target.getId())) {
            ConcurrentHashMap<String, Hint> hints = pending.computeIfAbsent(target.getId(),
                    id -> new ConcurrentHashMap<>());
            if (hints.size() >= MAX_HINTS_PER_NODE && !hints.containsKey(key)) {
                if (droppedHints.getAndIncrement() % 10_000 == 0) {
                    logger.warn("Hint limit reached for {}, dropping hints (anti-entropy will repair them)",
                            target.getId());
                }
                return;
            }
            hints.merge(key, hint, (old, fresh) -> fresh.timestamp > old.timestamp ? fresh : old);
            persist(target.getId(), hint);
            int records = recordsInFile.merge(target.getId(), 1, Integer::sum);
            if (records >= MIN_RECORDS_BEFORE_COMPACTION && records > COMPACTION_RATIO * hints.size()) {
                compact(target.getId(), hints);
            }
        }
    }

    /**
     * @return hints waiting to be delivered to the given node
     */
    public int pendingHints(String nodeId) {
        Map<String, Hint> hints = pending.get(nodeId);
        return hints != null ? hints.size() : 0;
    }

    public int totalPendingHints() {
        return pending.values().stream().mapToInt(Map::size).sum();
    }

    private void deliverAll() {
        for (String nodeId : pending.keySet()) {
            Node node = clusterManager.getNode(nodeId);
            if (node != null) {
                deliver(node);
            }
        }
    }

    /**
     * Send every pending hint to the node, stopping at the first failure; the rest are retried later.
     */
    private void deliver(Node target) {
        ConcurrentHashMap<String, Hint> hints = pending.get(target.getId());
        if (hints == null || hints.isEmpty() || !target.isAvailable() || !delivering.add(target.getId())) {
            return;
        }
        int delivered = 0;
        try {
            for (Hint hint : hints.values()) {
                replicaIO.write(target, hint.key, hint.value, hint.timestamp, hint.expiresAt);
                hints.remove(hint.key, hint); // unless a newer hint replaced it meanwhile
                delivered++;
            }
        } catch (IOException | RuntimeException e) {
            logger.debug("Hint delivery to {} paused after {} hints: {}", target.getId(), delivered, e.getMessage());
        } finally {
            delivering.remove(target.getId());
        }
        synchronized (lockFor(target.getId())) {
            if (hints.isEmpty()) {
                truncate(target.getId());
            }
        }
        if (delivered > 0) {
            logger.info("Delivered {} hints to {} ({} still pending)", delivered, target.getId(), hints.size());
        }
    }

    private Object lockFor(String nodeId) {
        return locks.computeIfAbsent(nodeId, id -> new Object());
    }

    // ==================== Persistence ====================

    private void persist(String nodeId, Hint hint) {
        if (hintsDir == null) {
            return;
        }
        try {
            FileChannel channel = files.computeIfAbsent(nodeId, this::open);
            ByteBuffer record = encode(hint);
            while (record.hasRemaining()) {
                channel.write(record);
            }
        } catch (IOException | UncheckedIOException e) {
            logger.warn("Failed to persist hint for {}: {}", nodeId, e.getMessage());
        }
    }

    private void truncate(String nodeId) {
        FileChannel channel = files.get(nodeId);
        recordsInFile.remove(nodeId);
        if (channel != null) {
            try {
                channel.truncate(0);
                writeHeader(channel, nodeId);
            } catch (IOException e) {
                logger.warn("Failed to truncate hints for {}: {}", nodeId, e.getMessage());
            }
        }
    }

    /**
     * Rewrite a node's hint file with only its pending hints, dropping records for keys that were
     * overwritten since. Caller holds the node's lock.
     */
    private void compact(String nodeId, Map<String, Hint> hints) {
        if (hintsDir == null) {
            return;
        }
        truncate(nodeId);
        for (Hint hint : hints.values()) {
            persist(nodeId, hint);
        }
        recordsInFile.put(nodeId, hints.size());
        logger.debug("Compacted hint file for {} to {} records", nodeId, hints.size());
    }

    /**
     * Forget every hint for a node that left the cluster for good, and delete its file.
     */
    private void discard(String nodeId) {
        synchronized (lockFor(nodeId)) {
            Map<String, Hint> dropped = pending.remove(nodeId);
            recordsInFile.remove(nodeId);
            FileChannel channel = files.remove(nodeId);
            if (channel != null) {
                try {
                    channel.close();
                    Files.deleteIfExists(hintFile(nodeId));
                } catch (IOException e) {
                    logger.warn("Failed to delete hints for {}: {}", nodeId, e.getMessage());
                }
            }
            if (dropped != null && !dropped.isEmpty()) {
                logger.info("Dropped {} hints for removed node {}", dropped.size(), nodeId);
            }
        }
    }

    private FileChannel open(String nodeId) {
        try {
            Files.createDirectories(hintsDir);
            FileChannel channel = FileChannel.open(hintFile(nodeId), StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE, StandardOpenOption.APPEND);
            if (channel.size() == 0) {
                writeHeader(channel, nodeId);
            }
            return channel;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Each file starts with the target node id, since file names are sanitized.
     */
    private static void writeHeader(FileChannel channel, String nodeId) throws IOException {
        byte[] id = nodeId.getBytes(StandardCharsets.UTF_8);
        ByteBuffer header = ByteBuffer.allocate(4 + id.length).putInt(id.length).put(id);
        header.flip();
        while (header.hasRemaining()) {
            channel.write(header);
        }
    }

    private Path hintFile(String nodeId) {
        return hintsDir.resolve(nodeId.replaceAll("[^a-zA-Z0-9._-]", "_") + ".hints");
    }

    private void loadPersistedHints() {
        if (!Files.isDirectory(hintsDir)) {
            return;
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(hintsDir, "*.hints")) {
            for (Path file : stream) {
                String fileName = file.getFileName().toString();
                String nodeId = decodeNodeId(file);
                if (nodeId == null) {
                    continue;
                }
                ConcurrentHashMap<String, Hint> hints = pending.computeIfAbsent(nodeId, id -> new ConcurrentHashMap<>());
                java.util.List<Hint> records = readAll(file);
                for (Hint hint : records) {
                    hints.merge(hint.key, hint, (old, fresh) -> fresh.timestamp > old.timestamp ? fresh : old);
                }
                recordsInFile.put(nodeId, records.size());
                logger.info("Loaded {} hints from {}", hints.size(), fileName);
            }
        } catch (IOException e) {
            logger.warn("Failed to load hints from {}: {}", hintsDir, e.getMessage());
        }
    }

    private static String decodeNodeId(Path file) throws IOException {
        byte[] data = Files.readAllBytes(file);
        if (data.length < 4) {
            return null;
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int length = buffer.getInt();
        if (length <= 0 || length > buffer.remaining()) {
            return null;
        }
        byte[] id = new byte[length];
        buffer.get(id);
        return new String(id, StandardCharsets.UTF_8);
    }

    private static java.util.List<Hint> readAll(Path file) throws IOException {
        java.util.List<Hint> hints = new java.util.ArrayList<>();
        ByteBuffer buffer = ByteBuffer.wrap(Files.readAllBytes(file));
        try {
            int idLength = buffer.getInt();
            buffer.position(buffer.position() + idLength);
            while (buffer.remaining() >= 4) {
                int start = buffer.position();
                int keyLength = buffer.getInt();
                byte[] key = new byte[keyLength];
                buffer.get(key);
                int valueLength = buffer.getInt();
                byte[] value = null;
                if (valueLength >= 0) {
                    value = new byte[valueLength];
                    buffer.get(value);
                }
                long timestamp = buffer.getLong();
                long expiresAt = buffer.getLong();
                int end = buffer.position();
                int crc = buffer.getInt();
                CRC32 check = new CRC32();
                check.update(buffer.array(), start, end - start);
                if ((int) check.getValue() != crc) {
                    break;
                }
                hints.add(new Hint(new String(key, StandardCharsets.UTF_8), value, timestamp, expiresAt));
            }
        } catch (RuntimeException e) {
            // truncated tail from a crash mid-append: keep what was read
        }
        return hints;
    }

    private ByteBuffer encode(Hint hint) {
        byte[] key = hint.key.getBytes(StandardCharsets.UTF_8);
        int valueLength = hint.value != null ? hint.value.length : -1;
        ByteBuffer buffer = ByteBuffer.allocate(4 + key.length + 4 + Math.max(valueLength, 0) + 8 + 8 + 4);
        buffer.putInt(key.length).put(key).putInt(valueLength);
        if (hint.value != null) {
            buffer.put(hint.value);
        }
        buffer.putLong(hint.timestamp).putLong(hint.expiresAt);
        CRC32 crc = new CRC32();
        crc.update(buffer.array(), 0, buffer.position());
        buffer.putInt((int) crc.getValue());
        buffer.flip();
        return buffer;
    }

    public void shutdown() {
        scheduler.shutdownNow();
        for (FileChannel channel : files.values()) {
            try {
                channel.close();
            } catch (IOException e) {
                logger.debug("Error closing hint file: {}", e.getMessage());
            }
        }
    }
}
