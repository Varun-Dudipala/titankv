package com.titankv.core;

import com.titankv.network.protocol.BinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

/**
 * Thread-safe in-memory key-value store implementation using ConcurrentHashMap.
 * Supports TTL with automatic expiration cleanup and memory limits.
 */
public class InMemoryStore implements KVStore {

    private static final Logger logger = LoggerFactory.getLogger(InMemoryStore.class);
    private static final long DEFAULT_MAX_MEMORY_BYTES = 512 * 1024 * 1024; // 512MB
    private static final int ENTRY_OVERHEAD_BYTES = 48; // Estimated object overhead
    private static final int WAL_MAGIC = 0x544B564C; // "TKVL"
    private static final short WAL_VERSION = 1;
    private static final byte WAL_OP_PUT = 0x01;
    private static final byte WAL_OP_DELETE = 0x02;
    private static final byte WAL_OP_CLEAR = 0x03;
    private static final long DEFAULT_WAL_MAX_BYTES = 128L * 1024 * 1024; // 128MB

    private final ConcurrentHashMap<String, KeyValuePair> store;
    private final ScheduledExecutorService cleanupExecutor;
    private final long cleanupIntervalMs;
    private final long maxMemoryBytes;
    private final AtomicLong currentMemoryBytes;
    private final boolean walEnabled;
    private final boolean walFsync;
    private final long walMaxBytes;
    private final Path dataDir;
    private final Path walPath;
    private final Path snapshotPath;
    private final Object walLock = new Object();
    private FileChannel walChannel;
    private long walBytes;
    private volatile boolean loading;

    /**
     * Get max memory from environment/system property, or use default.
     * Checks: TITANKV_MAX_MEMORY_MB env var, titankv.max.memory.mb property
     */
    private static long getDefaultMaxMemoryBytes() {
        String envValue = System.getenv("TITANKV_MAX_MEMORY_MB");
        if (envValue != null && !envValue.isEmpty()) {
            try {
                long mb = Long.parseLong(envValue.trim());
                if (mb > 0) {
                    logger.info("Using TITANKV_MAX_MEMORY_MB={} MB", mb);
                    return mb * 1024 * 1024;
                }
            } catch (NumberFormatException e) {
                logger.warn("Invalid TITANKV_MAX_MEMORY_MB value: {}, using default", envValue);
            }
        }

        String propValue = System.getProperty("titankv.max.memory.mb");
        if (propValue != null && !propValue.isEmpty()) {
            try {
                long mb = Long.parseLong(propValue.trim());
                if (mb > 0) {
                    logger.info("Using titankv.max.memory.mb={} MB", mb);
                    return mb * 1024 * 1024;
                }
            } catch (NumberFormatException e) {
                logger.warn("Invalid titankv.max.memory.mb value: {}, using default", propValue);
            }
        }

        return DEFAULT_MAX_MEMORY_BYTES;
    }

    private static boolean isDevMode() {
        return "true".equalsIgnoreCase(System.getenv("TITANKV_DEV_MODE"))
                || "true".equalsIgnoreCase(System.getProperty("titankv.dev.mode"));
    }

    private static boolean getBooleanProperty(String envKey, String propKey, boolean fallback) {
        String value = System.getenv(envKey);
        if (value == null || value.isEmpty()) {
            value = System.getProperty(propKey);
        }
        if (value == null || value.isEmpty()) {
            return fallback;
        }
        return "true".equalsIgnoreCase(value) || "1".equals(value);
    }

    private static boolean getDefaultWalEnabled() {
        boolean devMode = isDevMode();
        return getBooleanProperty("TITANKV_WAL_ENABLED", "titankv.wal.enabled", !devMode);
    }

    private static boolean getDefaultWalFsync() {
        return getBooleanProperty("TITANKV_WAL_FSYNC", "titankv.wal.fsync", true);
    }

    private static long getDefaultWalMaxBytes() {
        String envValue = System.getenv("TITANKV_WAL_MAX_MB");
        if (envValue == null || envValue.isEmpty()) {
            envValue = System.getProperty("titankv.wal.max.mb");
        }
        if (envValue != null && !envValue.isEmpty()) {
            try {
                long mb = Long.parseLong(envValue.trim());
                if (mb > 0) {
                    return mb * 1024 * 1024;
                }
            } catch (NumberFormatException e) {
                logger.warn("Invalid WAL max size {}, using default", envValue);
            }
        }
        return DEFAULT_WAL_MAX_BYTES;
    }

    private static Path getDefaultDataDir() {
        String envValue = System.getenv("TITANKV_DATA_DIR");
        if (envValue == null || envValue.isEmpty()) {
            envValue = System.getProperty("titankv.data.dir");
        }
        if (envValue == null || envValue.isEmpty()) {
            envValue = "data";
        }
        return Path.of(envValue);
    }

    /**
     * Create a new in-memory store with default cleanup interval (1 minute) and
     * default memory limit.
     */
    public InMemoryStore() {
        this(60_000, getDefaultMaxMemoryBytes());
    }

    /**
     * Create a new in-memory store with custom cleanup interval and default memory
     * limit.
     *
     * @param cleanupIntervalMs interval between cleanup runs in milliseconds
     */
    public InMemoryStore(long cleanupIntervalMs) {
        this(cleanupIntervalMs, getDefaultMaxMemoryBytes());
    }

    /**
     * Create a new in-memory store with custom cleanup interval and memory limit.
     *
     * @param cleanupIntervalMs interval between cleanup runs in milliseconds
     * @param maxMemoryBytes    maximum memory usage in bytes
     */
    public InMemoryStore(long cleanupIntervalMs, long maxMemoryBytes) {
        if (cleanupIntervalMs <= 0) {
            throw new IllegalArgumentException("cleanupIntervalMs must be positive");
        }
        if (maxMemoryBytes <= 0) {
            throw new IllegalArgumentException("maxMemoryBytes must be positive");
        }
        this.store = new ConcurrentHashMap<>();
        this.cleanupIntervalMs = cleanupIntervalMs;
        this.maxMemoryBytes = maxMemoryBytes;
        this.currentMemoryBytes = new AtomicLong(0);
        this.walEnabled = getDefaultWalEnabled();
        this.walFsync = getDefaultWalFsync();
        this.walMaxBytes = getDefaultWalMaxBytes();
        this.dataDir = getDefaultDataDir();
        this.walPath = dataDir.resolve("wal.log");
        this.snapshotPath = dataDir.resolve("snapshot.dat");
        this.loading = false;
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "titankv-cleanup");
            t.setDaemon(true);
            return t;
        });
        if (walEnabled) {
            initializeWal();
        }
        startCleanupTask();
    }

    private void startCleanupTask() {
        cleanupExecutor.scheduleAtFixedRate(this::cleanupExpired,
                cleanupIntervalMs, cleanupIntervalMs, TimeUnit.MILLISECONDS);
        logger.debug("Started TTL cleanup task with interval {}ms", cleanupIntervalMs);
    }

    private void initializeWal() {
        try {
            Files.createDirectories(dataDir);
            loading = true;
            loadSnapshotIfPresent();
            loadWalIfPresent(walPath);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to initialize WAL directory: " + dataDir, e);
        } finally {
            loading = false;
        }
        try {
            walChannel = FileChannel.open(walPath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.APPEND);
            walBytes = walChannel.size();
            logger.info("WAL enabled at {} (size={} bytes)", walPath.toAbsolutePath(), walBytes);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to open WAL at " + walPath, e);
        }
    }

    private void loadSnapshotIfPresent() {
        if (!Files.exists(snapshotPath)) {
            return;
        }
        try (FileChannel channel = FileChannel.open(snapshotPath, StandardOpenOption.READ)) {
            replayLog(channel);
            logger.info("Loaded snapshot from {}", snapshotPath.toAbsolutePath());
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load snapshot: " + snapshotPath, e);
        }
    }

    private void loadWalIfPresent(Path path) {
        if (!Files.exists(path)) {
            return;
        }
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            replayLog(channel);
            logger.info("Replayed WAL from {}", path.toAbsolutePath());
        } catch (IOException e) {
            throw new IllegalStateException("Failed to replay WAL: " + path, e);
        }
    }

    private void replayLog(FileChannel channel) throws IOException {
        ByteBuffer header = ByteBuffer.allocate(4 + 2 + 1 + 4 + 4 + 8 + 8);
        while (true) {
            header.clear();
            int read = readFully(channel, header);
            if (read == -1) {
                break;
            }
            if (read < header.capacity()) {
                logger.warn("Truncated WAL record (header), stopping replay");
                break;
            }
            header.flip();
            int magic = header.getInt();
            short version = header.getShort();
            byte op = header.get();
            int keyLen = header.getInt();
            int valueLen = header.getInt();
            long timestamp = header.getLong();
            long expiresAt = header.getLong();

            if (magic != WAL_MAGIC || version != WAL_VERSION) {
                logger.warn("Invalid WAL header (magic/version), stopping replay");
                break;
            }
            if (keyLen < 0 || keyLen > BinaryProtocol.MAX_KEY_LENGTH) {
                logger.warn("Invalid WAL key length {}, stopping replay", keyLen);
                break;
            }
            if (valueLen < -1 || valueLen > BinaryProtocol.MAX_VALUE_LENGTH) {
                logger.warn("Invalid WAL value length {}, stopping replay", valueLen);
                break;
            }

            int valueSize = valueLen > 0 ? valueLen : 0;
            int payloadSize = keyLen + valueSize + 4;
            ByteBuffer payload = ByteBuffer.allocate(payloadSize);
            int payloadRead = readFully(channel, payload);
            if (payloadRead < payloadSize) {
                logger.warn("Truncated WAL record (payload), stopping replay");
                break;
            }
            payload.flip();
            byte[] keyBytes = new byte[keyLen];
            payload.get(keyBytes);
            byte[] value = null;
            if (valueLen >= 0) {
                value = new byte[valueLen];
                if (valueLen > 0) {
                    payload.get(value);
                }
            }
            int checksum = payload.getInt();
            CRC32 crc = new CRC32();
            crc.update(header.array(), 0, header.capacity());
            crc.update(keyBytes, 0, keyBytes.length);
            if (valueSize > 0) {
                crc.update(value, 0, value.length);
            }
            if ((int) crc.getValue() != checksum) {
                logger.warn("WAL checksum mismatch, stopping replay");
                break;
            }

            String key = new String(keyBytes, StandardCharsets.UTF_8);
            applyRecord(op, key, value, timestamp, expiresAt);
        }
    }

    private int readFully(FileChannel channel, ByteBuffer buffer) throws IOException {
        int total = 0;
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer);
            if (read == -1) {
                return total == 0 ? -1 : total;
            }
            total += read;
        }
        return total;
    }

    private void applyRecord(byte op, String key, byte[] value, long timestamp, long expiresAt) {
        if (op == WAL_OP_PUT) {
            if (expiresAt > 0 && System.currentTimeMillis() > expiresAt) {
                return;
            }
            KeyValuePair entry = new KeyValuePair(value, timestamp, expiresAt);
            applyPut(key, entry);
            return;
        }
        if (op == WAL_OP_DELETE) {
            applyDelete(key);
            return;
        }
        if (op == WAL_OP_CLEAR) {
            store.clear();
            currentMemoryBytes.set(0);
            return;
        }
        logger.warn("Unknown WAL op {}, skipping", op);
    }

    private void applyPut(String key, KeyValuePair entry) {
        if (key == null || key.isEmpty()) {
            return;
        }
        long entrySize = estimateSize(key, entry.getValueUnsafe());
        while (currentMemoryBytes.get() + entrySize > maxMemoryBytes) {
            if (!evictOne()) {
                logger.warn("Memory limit exceeded during WAL replay for key {}", key);
                break;
            }
        }
        KeyValuePair previous = store.put(key, entry);
        if (previous != null) {
            currentMemoryBytes.addAndGet(-estimateSize(key, previous.getValueUnsafe()));
        }
        currentMemoryBytes.addAndGet(entrySize);
    }

    private void applyDelete(String key) {
        KeyValuePair removed = store.remove(key);
        if (removed != null) {
            currentMemoryBytes.addAndGet(-estimateSize(key, removed.getValueUnsafe()));
        }
    }

    private void writeWalRecord(byte op, String key, byte[] value, long timestamp, long expiresAt) {
        if (!walEnabled || loading) {
            return;
        }
        synchronized (walLock) {
            try {
                int bytesWritten = writeRecordToChannel(walChannel, op, key, value, timestamp, expiresAt);
                walBytes += bytesWritten;
                if (walFsync) {
                    walChannel.force(true);
                }
                if (walBytes >= walMaxBytes) {
                    snapshotLocked();
                }
            } catch (IOException e) {
                throw new IllegalStateException("WAL write failed", e);
            }
        }
    }

    private void snapshotLocked() {
        if (!walEnabled) {
            return;
        }
        Path tempSnapshot = snapshotPath.resolveSibling("snapshot.tmp");
        try (FileChannel snapshotChannel = FileChannel.open(tempSnapshot,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE)) {
            for (var entry : store.entrySet()) {
                KeyValuePair value = entry.getValue();
                if (value.isExpired()) {
                    continue;
                }
                writeRecordToChannel(snapshotChannel, WAL_OP_PUT,
                        entry.getKey(),
                        value.getValueUnsafe(),
                        value.getTimestamp(),
                        value.getExpiresAt());
            }
            snapshotChannel.force(true);
            Files.move(tempSnapshot, snapshotPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            walChannel.truncate(0);
            walChannel.position(0);
            walBytes = 0;
            logger.info("Snapshot written to {}", snapshotPath.toAbsolutePath());
        } catch (IOException e) {
            logger.warn("Snapshot failed: {}", e.getMessage());
        }
    }

    private int writeRecordToChannel(FileChannel channel, byte op, String key, byte[] value,
            long timestamp, long expiresAt) throws IOException {
        byte[] keyBytes = key != null ? key.getBytes(StandardCharsets.UTF_8) : new byte[0];
        int valueLen = value != null ? value.length : -1;
        int valueSize = valueLen > 0 ? valueLen : 0;
        int headerSize = 4 + 2 + 1 + 4 + 4 + 8 + 8;
        int totalSize = headerSize + keyBytes.length + valueSize + 4;
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.putInt(WAL_MAGIC);
        buffer.putShort(WAL_VERSION);
        buffer.put(op);
        buffer.putInt(keyBytes.length);
        buffer.putInt(valueLen);
        buffer.putLong(timestamp);
        buffer.putLong(expiresAt);
        buffer.put(keyBytes);
        if (valueSize > 0) {
            buffer.put(value);
        }
        CRC32 crc = new CRC32();
        crc.update(buffer.array(), 0, buffer.position());
        buffer.putInt((int) crc.getValue());
        buffer.flip();
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
        return totalSize;
    }

    /**
     * Remove all expired entries and update memory tracking.
     * Uses conditional remove to avoid race condition where value is updated
     * between check and remove.
     */
    private void cleanupExpired() {
        int removed = 0;
        for (var entry : store.entrySet()) {
            KeyValuePair value = entry.getValue();
            if (value.isExpired()) {
                // Use conditional remove to only remove if the value hasn't changed
                if (store.remove(entry.getKey(), value)) {
                    currentMemoryBytes.addAndGet(-estimateSize(entry.getKey(), value.getValueUnsafe()));
                    removed++;
                }
            }
        }
        if (removed > 0) {
            logger.debug("Cleaned up {} expired entries, memoryUsed={}", removed, currentMemoryBytes.get());
        }
    }

    @Override
    public Optional<KeyValuePair> put(String key, byte[] value) {
        return put(key, value, 0);
    }

    @Override
    public Optional<KeyValuePair> put(String key, byte[] value, long ttlMillis) {
        validateKey(key);

        long now = System.currentTimeMillis();
        long expiresAt = ttlMillis > 0 ? now + ttlMillis : 0;
        long entrySize = estimateSize(key, value);

        // Try to evict expired entries if we're over the limit
        while (currentMemoryBytes.get() + entrySize > maxMemoryBytes) {
            if (!evictOne()) {
                throw new IllegalStateException("Store memory limit exceeded and no entries to evict");
            }
        }

        writeWalRecord(WAL_OP_PUT, key, value, now, expiresAt);
        KeyValuePair newEntry = new KeyValuePair(value, now, expiresAt);
        KeyValuePair previous = store.put(key, newEntry);

        // Update memory tracking
        if (previous != null) {
            currentMemoryBytes.addAndGet(-estimateSize(key, previous.getValueUnsafe()));
        }
        currentMemoryBytes.addAndGet(entrySize);

        logger.trace("PUT key={}, valueSize={}, ttl={}, memoryUsed={}",
                key, value != null ? value.length : 0, ttlMillis, currentMemoryBytes.get());
        return Optional.ofNullable(previous);
    }

    /**
     * Put a value only if the provided timestamp is newer than existing (or no
     * existing value).
     * This is used for replication and read repair to maintain consistent conflict
     * resolution.
     *
     * @param key       the key
     * @param value     the value (null for tombstone)
     * @param timestamp the authoritative timestamp from the source
     * @param expiresAt the expiration timestamp (0 = no expiration)
     * @return true if the value was written, false if existing value is newer
     */
    public boolean putIfNewer(String key, byte[] value, long timestamp, long expiresAt) {
        validateKey(key);

        // Check existing value
        KeyValuePair existing = store.get(key);
        if (existing != null && existing.getTimestamp() >= timestamp) {
            logger.trace("PUT_IF_NEWER rejected: existing timestamp {} >= new timestamp {}",
                    existing.getTimestamp(), timestamp);
            return false;
        }

        long entrySize = estimateSize(key, value);

        // Try to evict if needed
        while (currentMemoryBytes.get() + entrySize > maxMemoryBytes) {
            if (!evictOne()) {
                throw new IllegalStateException("Store memory limit exceeded and no entries to evict");
            }
        }

        // Create entry with explicit timestamp and expiry
        KeyValuePair newEntry = new KeyValuePair(value, timestamp, expiresAt);

        // Capture old value for memory accounting (compute returns final value, not
        // previous)
        final KeyValuePair[] oldValueHolder = new KeyValuePair[1];

        // Use compute to ensure atomicity
        KeyValuePair result = store.compute(key, (k, old) -> {
            // Double-check timestamp during compute (race condition protection)
            if (old != null && old.getTimestamp() >= timestamp) {
                return old; // Keep existing
            }
            writeWalRecord(WAL_OP_PUT, key, value, timestamp, expiresAt);
            // Capture old value before replacing
            oldValueHolder[0] = old;
            return newEntry;
        });

        // If we successfully wrote newEntry (result is our new entry), update memory
        if (result == newEntry) {
            // Update memory tracking using the captured old value
            KeyValuePair oldValue = oldValueHolder[0];
            if (oldValue != null) {
                currentMemoryBytes.addAndGet(-estimateSize(key, oldValue.getValueUnsafe()));
            }
            currentMemoryBytes.addAndGet(entrySize);

            logger.trace("PUT_IF_NEWER key={}, timestamp={}, expiresAt={}, memoryUsed={}",
                    key, timestamp, expiresAt, currentMemoryBytes.get());
            return true;
        }

        return false;
    }

    /**
     * Estimate the memory size of a key-value entry.
     */
    private long estimateSize(String key, byte[] value) {
        long keySize = key.length() * 2L; // UTF-16 chars
        long valueSize = value != null ? value.length : 0;
        return keySize + valueSize + ENTRY_OVERHEAD_BYTES;
    }

    /**
     * Try to evict one entry (expired first, then oldest by timestamp).
     * Uses conditional remove to avoid race where value is updated between check
     * and remove.
     *
     * Eviction policy:
     * 1. First pass: evict any expired entries
     * 2. Second pass: evict oldest entry by timestamp (LRU-like)
     *
     * @return true if an entry was evicted, false if store is empty
     */
    private boolean evictOne() {
        // First pass: try to evict expired entries
        for (var entry : store.entrySet()) {
            KeyValuePair value = entry.getValue();
            if (value.isExpired()) {
                // Use conditional remove to only remove if the value hasn't changed
                if (store.remove(entry.getKey(), value)) {
                    currentMemoryBytes.addAndGet(-estimateSize(entry.getKey(), value.getValueUnsafe()));
                    logger.debug("Evicted expired key: {}", entry.getKey());
                    return true;
                }
            }
        }

        // Second pass: no expired entries, evict oldest by timestamp
        String oldestKey = null;
        KeyValuePair oldestValue = null;
        long oldestTimestamp = Long.MAX_VALUE;

        for (var entry : store.entrySet()) {
            KeyValuePair value = entry.getValue();
            if (value.getTimestamp() < oldestTimestamp) {
                oldestTimestamp = value.getTimestamp();
                oldestKey = entry.getKey();
                oldestValue = value;
            }
        }

        if (oldestKey != null && oldestValue != null) {
            // Use conditional remove in case value was updated during iteration
            if (store.remove(oldestKey, oldestValue)) {
                currentMemoryBytes.addAndGet(-estimateSize(oldestKey, oldestValue.getValueUnsafe()));
                logger.debug("Evicted oldest key (timestamp={}): {}", oldestTimestamp, oldestKey);
                return true;
            }
        }

        // Store is empty or all removals raced
        return false;
    }

    @Override
    public Optional<KeyValuePair> get(String key) {
        validateKey(key);
        KeyValuePair entry = store.get(key);
        if (entry == null) {
            logger.trace("GET key={} -> NOT_FOUND", key);
            return Optional.empty();
        }
        if (entry.isTombstone()) {
            logger.trace("GET key={} -> TOMBSTONE", key);
            return Optional.empty();
        }
        if (entry.isExpired()) {
            // Lazy deletion of expired entry
            if (store.remove(key, entry)) {
                currentMemoryBytes.addAndGet(-estimateSize(key, entry.getValueUnsafe()));
            }
            logger.trace("GET key={} -> EXPIRED", key);
            return Optional.empty();
        }
        logger.trace("GET key={} -> FOUND", key);
        return Optional.of(entry);
    }

    @Override
    public Optional<KeyValuePair> getRaw(String key) {
        validateKey(key);
        KeyValuePair entry = store.get(key);
        if (entry == null) {
            return Optional.empty();
        }
        if (entry.isExpired()) {
            if (store.remove(key, entry)) {
                currentMemoryBytes.addAndGet(-estimateSize(key, entry.getValueUnsafe()));
            }
            return Optional.empty();
        }
        return Optional.of(entry);
    }

    @Override
    public Optional<KeyValuePair> delete(String key) {
        validateKey(key);
        writeWalRecord(WAL_OP_DELETE, key, null, System.currentTimeMillis(), 0);
        KeyValuePair removed = store.remove(key);
        if (removed != null) {
            currentMemoryBytes.addAndGet(-estimateSize(key, removed.getValueUnsafe()));
        }
        logger.trace("DELETE key={} -> {}", key, removed != null ? "DELETED" : "NOT_FOUND");
        return Optional.ofNullable(removed);
    }

    @Override
    public boolean exists(String key) {
        validateKey(key);
        KeyValuePair entry = store.get(key);
        if (entry == null) {
            return false;
        }
        if (entry.isTombstone()) {
            return false;
        }
        if (entry.isExpired()) {
            if (store.remove(key, entry)) {
                currentMemoryBytes.addAndGet(-estimateSize(key, entry.getValueUnsafe()));
            }
            return false;
        }
        return true;
    }

    @Override
    public Set<String> keys() {
        return store.entrySet().stream()
                .filter(e -> !e.getValue().isExpired() && !e.getValue().isTombstone())
                .map(e -> e.getKey())
                .collect(Collectors.toSet());
    }

    @Override
    public int size() {
        return (int) store.entrySet().stream()
                .filter(e -> !e.getValue().isExpired() && !e.getValue().isTombstone())
                .count();
    }

    @Override
    public void clear() {
        writeWalRecord(WAL_OP_CLEAR, "", null, 0, 0);
        store.clear();
        currentMemoryBytes.set(0);
        logger.debug("Store cleared");
    }

    /**
     * Shutdown the cleanup executor.
     */
    public void shutdown() {
        cleanupExecutor.shutdown();
        try {
            if (!cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                cleanupExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            cleanupExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        if (walChannel != null) {
            try {
                walChannel.close();
            } catch (IOException e) {
                logger.debug("Error closing WAL: {}", e.getMessage());
            }
        }
        logger.info("InMemoryStore shutdown complete");
    }

    /**
     * Put with explicit timestamp for replication.
     * Only updates if the new entry is newer than existing.
     *
     * @param key   the key
     * @param entry the entry with timestamp
     * @return true if the entry was stored
     */
    public boolean putIfNewer(String key, KeyValuePair entry) {
        validateKey(key);

        KeyValuePair existing = store.get(key);
        if (!entry.isNewerThan(existing)) {
            return false;
        }

        long entrySize = estimateSize(key, entry.getValueUnsafe());

        // Try to evict if needed - reusing evictOne logic from other methods
        while (currentMemoryBytes.get() + entrySize > maxMemoryBytes) {
            if (!evictOne()) {
                logger.warn("Memory limit exceeded during replication write for key {}", key);
                return false;
            }
        }

        // Track whether entry was actually stored
        final boolean[] wasStored = { false };

        store.compute(key, (k, current) -> {
            if (entry.isNewerThan(current)) {
                writeWalRecord(WAL_OP_PUT, key, entry.getValueUnsafe(), entry.getTimestamp(), entry.getExpiresAt());
                // Update memory accounting
                long oldSize = current != null ? estimateSize(k, current.getValueUnsafe()) : 0;
                long newSize = estimateSize(k, entry.getValueUnsafe());
                currentMemoryBytes.addAndGet(newSize - oldSize);

                wasStored[0] = true;
                return entry;
            }
            return current;
        });

        return wasStored[0];
    }

    /**
     * Get raw entry count including expired (for debugging).
     */
    public int rawSize() {
        return store.size();
    }

    private void validateKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }
    }
}
