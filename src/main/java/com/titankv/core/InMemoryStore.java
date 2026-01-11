package com.titankv.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Thread-safe in-memory key-value store implementation using ConcurrentHashMap.
 * Supports TTL with automatic expiration cleanup.
 */
public class InMemoryStore implements KVStore {

    private static final Logger logger = LoggerFactory.getLogger(InMemoryStore.class);

    private final ConcurrentHashMap<String, KeyValuePair> store;
    private final ScheduledExecutorService cleanupExecutor;
    private final long cleanupIntervalMs;

    /**
     * Create a new in-memory store with default cleanup interval (1 minute).
     */
    public InMemoryStore() {
        this(60_000);
    }

    /**
     * Create a new in-memory store with custom cleanup interval.
     *
     * @param cleanupIntervalMs interval between cleanup runs in milliseconds
     */
    public InMemoryStore(long cleanupIntervalMs) {
        this.store = new ConcurrentHashMap<>();
        this.cleanupIntervalMs = cleanupIntervalMs;
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "titankv-cleanup");
            t.setDaemon(true);
            return t;
        });
        startCleanupTask();
    }

    private void startCleanupTask() {
        cleanupExecutor.scheduleAtFixedRate(this::cleanupExpired,
            cleanupIntervalMs, cleanupIntervalMs, TimeUnit.MILLISECONDS);
        logger.debug("Started TTL cleanup task with interval {}ms", cleanupIntervalMs);
    }

    /**
     * Remove all expired entries.
     */
    private void cleanupExpired() {
        int removed = 0;
        for (var entry : store.entrySet()) {
            if (entry.getValue().isExpired()) {
                store.remove(entry.getKey(), entry.getValue());
                removed++;
            }
        }
        if (removed > 0) {
            logger.debug("Cleaned up {} expired entries", removed);
        }
    }

    @Override
    public Optional<KeyValuePair> put(String key, byte[] value) {
        return put(key, value, 0);
    }

    @Override
    public Optional<KeyValuePair> put(String key, byte[] value, long ttlMillis) {
        validateKey(key);
        KeyValuePair newEntry = new KeyValuePair(value, ttlMillis);
        KeyValuePair previous = store.put(key, newEntry);
        logger.trace("PUT key={}, valueSize={}, ttl={}", key, value != null ? value.length : 0, ttlMillis);
        return Optional.ofNullable(previous);
    }

    @Override
    public Optional<KeyValuePair> get(String key) {
        validateKey(key);
        KeyValuePair entry = store.get(key);
        if (entry == null) {
            logger.trace("GET key={} -> NOT_FOUND", key);
            return Optional.empty();
        }
        if (entry.isExpired()) {
            // Lazy deletion of expired entry
            store.remove(key, entry);
            logger.trace("GET key={} -> EXPIRED", key);
            return Optional.empty();
        }
        logger.trace("GET key={} -> FOUND", key);
        return Optional.of(entry);
    }

    @Override
    public Optional<KeyValuePair> delete(String key) {
        validateKey(key);
        KeyValuePair removed = store.remove(key);
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
        if (entry.isExpired()) {
            store.remove(key, entry);
            return false;
        }
        return true;
    }

    @Override
    public Set<String> keys() {
        return store.entrySet().stream()
            .filter(e -> !e.getValue().isExpired())
            .map(e -> e.getKey())
            .collect(Collectors.toSet());
    }

    @Override
    public int size() {
        return (int) store.entrySet().stream()
            .filter(e -> !e.getValue().isExpired())
            .count();
    }

    @Override
    public void clear() {
        store.clear();
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
        logger.info("InMemoryStore shutdown complete");
    }

    /**
     * Put with explicit timestamp for replication.
     * Only updates if the new entry is newer than existing.
     *
     * @param key       the key
     * @param entry     the entry with timestamp
     * @return true if the entry was stored
     */
    public boolean putIfNewer(String key, KeyValuePair entry) {
        validateKey(key);
        return store.compute(key, (k, existing) -> {
            if (entry.isNewerThan(existing)) {
                return entry;
            }
            return existing;
        }) == entry;
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
