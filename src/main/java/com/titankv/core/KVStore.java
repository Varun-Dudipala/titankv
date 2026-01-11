package com.titankv.core;

import java.util.Optional;
import java.util.Set;

/**
 * Core storage interface for the key-value store.
 * All implementations must be thread-safe.
 */
public interface KVStore {

    /**
     * Store a value with the given key.
     *
     * @param key   the key to store
     * @param value the value to store
     * @return the previous value if one existed, empty otherwise
     */
    Optional<KeyValuePair> put(String key, byte[] value);

    /**
     * Store a value with the given key and TTL.
     *
     * @param key      the key to store
     * @param value    the value to store
     * @param ttlMillis time-to-live in milliseconds (0 for no expiration)
     * @return the previous value if one existed, empty otherwise
     */
    Optional<KeyValuePair> put(String key, byte[] value, long ttlMillis);

    /**
     * Retrieve the value for a given key.
     *
     * @param key the key to look up
     * @return the value if found and not expired, empty otherwise
     */
    Optional<KeyValuePair> get(String key);

    /**
     * Delete a key-value pair.
     *
     * @param key the key to delete
     * @return the deleted value if one existed, empty otherwise
     */
    Optional<KeyValuePair> delete(String key);

    /**
     * Check if a key exists and is not expired.
     *
     * @param key the key to check
     * @return true if the key exists and is not expired
     */
    boolean exists(String key);

    /**
     * Get all keys in the store.
     *
     * @return set of all non-expired keys
     */
    Set<String> keys();

    /**
     * Get the number of entries in the store.
     *
     * @return the number of non-expired entries
     */
    int size();

    /**
     * Clear all entries from the store.
     */
    void clear();
}
