package com.titankv.core;

import com.titankv.network.protocol.BinaryProtocol;
import com.titankv.util.Env;
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
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

/**
 * Thread-safe in-memory key-value store backed by a ConcurrentHashMap, with optional
 * write-ahead log (WAL) and snapshots for crash recovery, TTL expiry and a memory limit.
 *
 * Durability: every mutation appends a CRC-checked record to the WAL (fsynced by default) before
 * it is acknowledged. When the WAL grows past its limit the store writes a snapshot of all live
 * entries and truncates the WAL. On startup the snapshot and then the WAL are replayed.
 *
 * Invariants:
 *  - A key's WAL record is appended inside that key's map update, so for any one key the WAL
 *    order matches the order updates were applied in memory.
 *  - Mutations hold the read side of {@code mutationLock}; a snapshot holds the write side. So
 *    when the WAL is truncated, every record in it has already been applied to the map and is
 *    therefore captured by the snapshot.
 *  - Group commit: writers append under {@code walLock} but fsync outside it. A writer waits
 *    until the WAL is durable up to its own record; one fsync covers every record appended
 *    before it started, so concurrent writers share fsyncs instead of queueing for one each.
 */
public class InMemoryStore implements KVStore {

    private static final Logger logger = LoggerFactory.getLogger(InMemoryStore.class);
    private static final long DEFAULT_MAX_MEMORY_BYTES = 512L * 1024 * 1024;
    private static final int ENTRY_OVERHEAD_BYTES = 48; // estimated per-entry object overhead
    private static final int WAL_MAGIC = 0x544B564C; // "TKVL"
    private static final short WAL_VERSION = 1;
    private static final byte WAL_OP_PUT = 0x01;
    private static final byte WAL_OP_DELETE = 0x02;
    private static final byte WAL_OP_CLEAR = 0x03;
    private static final int WAL_HEADER_SIZE = 4 + 2 + 1 + 4 + 4 + 8 + 8;
    private static final long DEFAULT_WAL_MAX_BYTES = 128L * 1024 * 1024;
    private static final long DEFAULT_TOMBSTONE_GRACE_MS = 24L * 60 * 60 * 1000;

    private final ConcurrentHashMap<String, KeyValuePair> store = new ConcurrentHashMap<>();
    private final ScheduledExecutorService cleanupExecutor;
    private final long maxMemoryBytes;
    private final AtomicLong currentMemoryBytes = new AtomicLong();
    private final long tombstoneGraceMs;
    private final boolean walEnabled;
    private final boolean walFsync;
    private final long walMaxBytes;
    private final Path dataDir;
    private final Path walPath;
    private final Path snapshotPath;
    private final Object walLock = new Object();
    private final Object syncLock = new Object();
    // Bytes ever appended / known durable, never reset by snapshots. Used for group commit.
    private long appendedTotal;
    private volatile long durableTotal;
    private final ThreadLocal<long[]> lastAppendEnd = ThreadLocal.withInitial(() -> new long[1]);
    private final ReentrantReadWriteLock mutationLock = new ReentrantReadWriteLock();
    private FileChannel walChannel;
    private long walBytes;
    private boolean loading;

    public InMemoryStore() {
        this(60_000, defaultMaxMemoryBytes(), defaultDataDir());
    }

    public InMemoryStore(long cleanupIntervalMs) {
        this(cleanupIntervalMs, defaultMaxMemoryBytes(), defaultDataDir());
    }

    public InMemoryStore(long cleanupIntervalMs, long maxMemoryBytes) {
        this(cleanupIntervalMs, maxMemoryBytes, defaultDataDir());
    }

    public InMemoryStore(Path dataDir) {
        this(60_000, defaultMaxMemoryBytes(), dataDir);
    }

    /**
     * @param cleanupIntervalMs how often expired entries and old tombstones are purged
     * @param maxMemoryBytes    writes that would exceed this estimated size are rejected
     * @param dataDir           directory for the WAL and snapshot (used only if the WAL is enabled)
     */
    public InMemoryStore(long cleanupIntervalMs, long maxMemoryBytes, Path dataDir) {
        if (cleanupIntervalMs <= 0) {
            throw new IllegalArgumentException("cleanupIntervalMs must be positive");
        }
        if (maxMemoryBytes <= 0) {
            throw new IllegalArgumentException("maxMemoryBytes must be positive");
        }
        this.maxMemoryBytes = maxMemoryBytes;
        this.tombstoneGraceMs = positiveLong("TITANKV_TOMBSTONE_GRACE_MS", "titankv.tombstone.grace.ms", 1,
                DEFAULT_TOMBSTONE_GRACE_MS);
        // The WAL defaults to on, except in dev mode where durability is not expected
        this.walEnabled = Env.getBoolean("TITANKV_WAL_ENABLED", "titankv.wal.enabled", !Env.isDevMode());
        this.walFsync = Env.getBoolean("TITANKV_WAL_FSYNC", "titankv.wal.fsync", true);
        this.walMaxBytes = positiveLong("TITANKV_WAL_MAX_MB", "titankv.wal.max.mb", 1024 * 1024,
                DEFAULT_WAL_MAX_BYTES);
        this.dataDir = dataDir;
        this.walPath = dataDir.resolve("wal.log");
        this.snapshotPath = dataDir.resolve("snapshot.dat");
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "titankv-cleanup");
            t.setDaemon(true);
            return t;
        });
        if (walEnabled) {
            recover();
        }
        cleanupExecutor.scheduleAtFixedRate(this::cleanupExpired, cleanupIntervalMs, cleanupIntervalMs,
                TimeUnit.MILLISECONDS);
    }

    private static long defaultMaxMemoryBytes() {
        return positiveLong("TITANKV_MAX_MEMORY_MB", "titankv.max.memory.mb", 1024 * 1024, DEFAULT_MAX_MEMORY_BYTES);
    }

    private static Path defaultDataDir() {
        String dir = Env.get("TITANKV_DATA_DIR", "titankv.data.dir");
        return Path.of(dir != null ? dir : "data");
    }

    /**
     * @return the configured value times {@code unit}, or the fallback if unset or invalid
     */
    private static long positiveLong(String envKey, String propKey, long unit, long fallback) {
        String value = Env.get(envKey, propKey);
        if (value == null) {
            return fallback;
        }
        try {
            long parsed = Long.parseLong(value.trim());
            if (parsed > 0) {
                return parsed * unit;
            }
        } catch (NumberFormatException e) {
            // fall through
        }
        logger.warn("Invalid value {} for {}, using default", value, envKey);
        return fallback;
    }

    // ==================== Recovery ====================

    private void recover() {
        try {
            Files.createDirectories(dataDir);
            loading = true;
            if (Files.exists(snapshotPath)) {
                replayFile(snapshotPath);
                logger.info("Loaded snapshot from {}", snapshotPath.toAbsolutePath());
            }
            if (Files.exists(walPath)) {
                replayFile(walPath);
                logger.info("Replayed WAL from {}", walPath.toAbsolutePath());
            }
            walChannel = FileChannel.open(walPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                    StandardOpenOption.APPEND);
            walBytes = walChannel.size();
            logger.info("WAL enabled at {} (size={} bytes, {} keys recovered)",
                    walPath.toAbsolutePath(), walBytes, store.size());
        } catch (IOException e) {
            throw new IllegalStateException("Failed to initialize WAL in " + dataDir, e);
        } finally {
            loading = false;
        }
    }

    /**
     * Apply every intact record in order. Stops at the first truncated or corrupt record, which
     * is what a crash in the middle of an append leaves behind.
     */
    private void replayFile(Path path) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            ByteBuffer header = ByteBuffer.allocate(WAL_HEADER_SIZE);
            while (true) {
                header.clear();
                int read = readFully(channel, header);
                if (read == -1) {
                    return;
                }
                if (read < WAL_HEADER_SIZE) {
                    logger.warn("Truncated record header in {}, stopping replay", path);
                    return;
                }
                header.flip();
                int magic = header.getInt();
                short version = header.getShort();
                byte op = header.get();
                int keyLen = header.getInt();
                int valueLen = header.getInt();
                long timestamp = header.getLong();
                long expiresAt = header.getLong();

                if (magic != WAL_MAGIC || version != WAL_VERSION
                        || keyLen < 0 || keyLen > BinaryProtocol.MAX_KEY_LENGTH
                        || valueLen < -1 || valueLen > BinaryProtocol.MAX_VALUE_LENGTH) {
                    logger.warn("Corrupt record header in {}, stopping replay", path);
                    return;
                }

                int valueSize = Math.max(valueLen, 0);
                ByteBuffer payload = ByteBuffer.allocate(keyLen + valueSize + 4);
                if (readFully(channel, payload) < payload.capacity()) {
                    logger.warn("Truncated record in {}, stopping replay", path);
                    return;
                }
                payload.flip();
                byte[] keyBytes = new byte[keyLen];
                payload.get(keyBytes);
                byte[] value = valueLen >= 0 ? new byte[valueLen] : null;
                if (valueSize > 0) {
                    payload.get(value);
                }
                int checksum = payload.getInt();

                CRC32 crc = new CRC32();
                crc.update(header.array(), 0, WAL_HEADER_SIZE);
                crc.update(keyBytes);
                if (valueSize > 0) {
                    crc.update(value);
                }
                if ((int) crc.getValue() != checksum) {
                    logger.warn("Checksum mismatch in {}, stopping replay", path);
                    return;
                }
                applyRecord(op, new String(keyBytes, StandardCharsets.UTF_8), value, timestamp, expiresAt);
            }
        }
    }

    private static int readFully(FileChannel channel, ByteBuffer buffer) throws IOException {
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
        switch (op) {
            case WAL_OP_PUT:
                if (expiresAt > 0 && System.currentTimeMillis() > expiresAt) {
                    applyDelete(key);
                } else {
                    applyPut(key, new KeyValuePair(value, timestamp, expiresAt));
                }
                break;
            case WAL_OP_DELETE:
                applyDelete(key);
                break;
            case WAL_OP_CLEAR:
                store.clear();
                currentMemoryBytes.set(0);
                break;
            default:
                logger.warn("Unknown WAL op {}, skipping", op);
        }
    }

    private void applyPut(String key, KeyValuePair entry) {
        KeyValuePair previous = store.put(key, entry);
        adjustMemory(key, previous, entry);
    }

    private void applyDelete(String key) {
        adjustMemory(key, store.remove(key), null);
    }

    // ==================== WAL writing ====================

    /**
     * Run a mutation so it cannot interleave with a snapshot, then snapshot if the WAL is full.
     */
    private <T> T mutate(Supplier<T> mutation) {
        if (!walEnabled) {
            return mutation.get();
        }
        T result;
        mutationLock.readLock().lock();
        try {
            result = mutation.get();
            awaitDurable();
        } finally {
            mutationLock.readLock().unlock();
        }
        maybeSnapshot();
        return result;
    }

    private void appendToWal(byte op, String key, byte[] value, long timestamp, long expiresAt) {
        if (!walEnabled || loading) {
            return;
        }
        synchronized (walLock) {
            try {
                int written = writeRecord(walChannel, op, key, value, timestamp, expiresAt);
                walBytes += written;
                appendedTotal += written;
                lastAppendEnd.get()[0] = appendedTotal;
            } catch (IOException e) {
                throw new IllegalStateException("WAL write failed", e);
            }
        }
    }

    /**
     * Block until this thread's last WAL record is on disk. Whichever waiting writer gets the sync
     * lock fsyncs everything appended so far, so writers that arrive meanwhile are covered too.
     */
    private void awaitDurable() {
        long target = lastAppendEnd.get()[0];
        if (!walFsync || target <= durableTotal) {
            return;
        }
        synchronized (syncLock) {
            if (target <= durableTotal) {
                return;
            }
            long upTo;
            synchronized (walLock) {
                upTo = appendedTotal;
            }
            try {
                walChannel.force(false);
            } catch (IOException e) {
                throw new IllegalStateException("WAL fsync failed", e);
            }
            durableTotal = upTo;
        }
    }

    private void maybeSnapshot() {
        synchronized (walLock) {
            if (walBytes < walMaxBytes) {
                return;
            }
        }
        mutationLock.writeLock().lock();
        try {
            synchronized (walLock) {
                if (walBytes >= walMaxBytes) {
                    writeSnapshot();
                }
            }
        } finally {
            mutationLock.writeLock().unlock();
        }
    }

    /**
     * Write all live entries to a new snapshot, atomically replace the old one, then truncate the
     * WAL. Caller holds the mutation write lock, so no mutation is in flight.
     */
    private void writeSnapshot() {
        Path tempSnapshot = snapshotPath.resolveSibling("snapshot.tmp");
        try (FileChannel out = FileChannel.open(tempSnapshot, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            for (var entry : store.entrySet()) {
                KeyValuePair kv = entry.getValue();
                if (!kv.isExpired()) {
                    writeRecord(out, WAL_OP_PUT, entry.getKey(), kv.getValueUnsafe(), kv.getTimestamp(),
                            kv.getExpiresAt());
                }
            }
            out.force(true);
        } catch (IOException e) {
            logger.warn("Snapshot failed, keeping the WAL: {}", e.getMessage());
            return;
        }
        try {
            Files.move(tempSnapshot, snapshotPath, StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
            walChannel.truncate(0);
            walChannel.force(true);
            walBytes = 0;
            durableTotal = appendedTotal; // everything appended so far is in the fsynced snapshot
            logger.info("Snapshot of {} keys written to {}", store.size(), snapshotPath.toAbsolutePath());
        } catch (IOException e) {
            throw new IllegalStateException("Failed to install snapshot", e);
        }
    }

    private static int writeRecord(FileChannel channel, byte op, String key, byte[] value,
            long timestamp, long expiresAt) throws IOException {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        int valueLen = value != null ? value.length : -1;
        int valueSize = Math.max(valueLen, 0);
        ByteBuffer buffer = ByteBuffer.allocate(WAL_HEADER_SIZE + keyBytes.length + valueSize + 4);
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
        return buffer.capacity();
    }

    // ==================== Operations ====================

    @Override
    public Optional<KeyValuePair> put(String key, byte[] value) {
        return put(key, value, 0);
    }

    @Override
    public Optional<KeyValuePair> put(String key, byte[] value, long ttlMillis) {
        validateKey(key);
        long now = System.currentTimeMillis();
        KeyValuePair entry = new KeyValuePair(value, now, ttlMillis > 0 ? now + ttlMillis : 0);
        ensureCapacity(key, entry);
        return mutate(() -> {
            KeyValuePair[] previous = new KeyValuePair[1];
            store.compute(key, (k, old) -> {
                appendToWal(WAL_OP_PUT, key, value, entry.getTimestamp(), entry.getExpiresAt());
                previous[0] = old;
                return entry;
            });
            adjustMemory(key, previous[0], entry);
            return Optional.ofNullable(previous[0]);
        });
    }

    /**
     * Store a versioned value if it is newer than the current entry (last write wins). Used by
     * replication, read repair and deletes (a null value is a tombstone).
     */
    @Override
    public boolean putIfNewer(String key, byte[] value, long timestamp, long expiresAt) {
        return putIfNewer(key, new KeyValuePair(value, timestamp, expiresAt));
    }

    /**
     * Store the entry if it is newer than the current one.
     *
     * @return true if stored, false if the existing entry is at least as new
     */
    public boolean putIfNewer(String key, KeyValuePair entry) {
        validateKey(key);
        if (!entry.isNewerThan(store.get(key))) {
            return false;
        }
        ensureCapacity(key, entry);
        return mutate(() -> {
            KeyValuePair[] previous = new KeyValuePair[1];
            boolean[] stored = {false};
            store.compute(key, (k, current) -> {
                if (!entry.isNewerThan(current)) {
                    return current;
                }
                appendToWal(WAL_OP_PUT, key, entry.getValueUnsafe(), entry.getTimestamp(), entry.getExpiresAt());
                previous[0] = current;
                stored[0] = true;
                return entry;
            });
            if (stored[0]) {
                adjustMemory(key, previous[0], entry);
            }
            return stored[0];
        });
    }

    @Override
    public Optional<KeyValuePair> get(String key) {
        validateKey(key);
        KeyValuePair entry = liveEntry(key);
        return entry == null || entry.isTombstone() ? Optional.empty() : Optional.of(entry);
    }

    @Override
    public Optional<KeyValuePair> getRaw(String key) {
        validateKey(key);
        return Optional.ofNullable(liveEntry(key));
    }

    @Override
    public Optional<KeyValuePair> delete(String key) {
        validateKey(key);
        return mutate(() -> {
            KeyValuePair[] removed = new KeyValuePair[1];
            store.compute(key, (k, old) -> {
                appendToWal(WAL_OP_DELETE, key, null, System.currentTimeMillis(), 0);
                removed[0] = old;
                return null;
            });
            adjustMemory(key, removed[0], null);
            return Optional.ofNullable(removed[0]);
        });
    }

    @Override
    public boolean exists(String key) {
        return get(key).isPresent();
    }

    @Override
    public Set<String> keys() {
        return store.entrySet().stream()
                .filter(e -> !e.getValue().isExpired() && !e.getValue().isTombstone())
                .map(e -> e.getKey())
                .collect(Collectors.toSet());
    }

    /**
     * All unexpired keys, including deleted keys whose tombstones are still retained.
     */
    public Set<String> keysIncludingTombstones() {
        return store.entrySet().stream()
                .filter(e -> !e.getValue().isExpired())
                .map(e -> e.getKey())
                .collect(Collectors.toSet());
    }

    @Override
    public int size() {
        return (int) store.values().stream()
                .filter(v -> !v.isExpired() && !v.isTombstone())
                .count();
    }

    @Override
    public void clear() {
        if (walEnabled) {
            mutationLock.writeLock().lock();
        }
        try {
            appendToWal(WAL_OP_CLEAR, "", null, 0, 0);
            awaitDurable();
            store.clear();
            currentMemoryBytes.set(0);
        } finally {
            if (walEnabled) {
                mutationLock.writeLock().unlock();
            }
        }
    }

    /**
     * Number of stored entries including expired ones and tombstones.
     */
    public int rawSize() {
        return store.size();
    }

    /**
     * Estimated bytes used by stored entries.
     */
    public long getMemoryUsedBytes() {
        return currentMemoryBytes.get();
    }

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
        synchronized (walLock) {
            if (walChannel != null) {
                try {
                    walChannel.close();
                } catch (IOException e) {
                    logger.debug("Error closing WAL: {}", e.getMessage());
                }
            }
        }
        logger.info("InMemoryStore shutdown complete");
    }

    // ==================== Housekeeping ====================

    /**
     * The entry for a key, removing it first if it has expired.
     */
    private KeyValuePair liveEntry(String key) {
        KeyValuePair entry = store.get(key);
        if (entry != null && entry.isExpired()) {
            if (store.remove(key, entry)) {
                adjustMemory(key, entry, null);
            }
            return null;
        }
        return entry;
    }

    /**
     * Purge expired entries, and tombstones older than the grace period. Tombstones are kept that
     * long so a replica that missed the delete gets repaired instead of resurrecting the value.
     * Purges are not written to the WAL: replay re-derives expiry, and a replayed old tombstone is
     * purged again on the next run.
     */
    private void cleanupExpired() {
        long tombstoneCutoff = System.currentTimeMillis() - tombstoneGraceMs;
        int removed = 0;
        for (var entry : store.entrySet()) {
            KeyValuePair value = entry.getValue();
            boolean purge = value.isExpired() || (value.isTombstone() && value.getTimestamp() < tombstoneCutoff);
            if (purge && store.remove(entry.getKey(), value)) {
                adjustMemory(entry.getKey(), value, null);
                removed++;
            }
        }
        if (removed > 0) {
            logger.debug("Purged {} expired entries and old tombstones, memoryUsed={}", removed,
                    currentMemoryBytes.get());
        }
    }

    /**
     * Reject a write that would exceed the memory limit, after first purging expired entries.
     * Live data is never evicted: silently dropping acknowledged writes would be data loss.
     */
    private void ensureCapacity(String key, KeyValuePair entry) {
        long needed = estimateSize(key, entry.getValueUnsafe());
        if (currentMemoryBytes.get() + needed <= maxMemoryBytes) {
            return;
        }
        cleanupExpired();
        if (currentMemoryBytes.get() + needed > maxMemoryBytes) {
            throw new IllegalStateException("Store memory limit of " + maxMemoryBytes + " bytes reached");
        }
    }

    private void adjustMemory(String key, KeyValuePair removed, KeyValuePair added) {
        long delta = 0;
        if (removed != null) {
            delta -= estimateSize(key, removed.getValueUnsafe());
        }
        if (added != null) {
            delta += estimateSize(key, added.getValueUnsafe());
        }
        if (delta != 0) {
            currentMemoryBytes.addAndGet(delta);
        }
    }

    private static long estimateSize(String key, byte[] value) {
        return key.length() * 2L + (value != null ? value.length : 0) + ENTRY_OVERHEAD_BYTES;
    }

    private static void validateKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }
    }
}
