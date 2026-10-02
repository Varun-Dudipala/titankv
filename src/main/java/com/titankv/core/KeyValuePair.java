package com.titankv.core;

import com.titankv.util.HybridLogicalClock;

import java.util.Arrays;
import java.util.Objects;

/**
 * Immutable data model for stored values.
 * Contains the value bytes along with metadata for conflict resolution and expiration.
 */
public final class KeyValuePair {

    private final byte[] value; // null indicates tombstone
    private final long timestamp;
    private final long expiresAt; // 0 means no expiration

    /**
     * Create a new key-value pair with no expiration.
     *
     * @param value the value bytes
     */
    public KeyValuePair(byte[] value) {
        this(value, HybridLogicalClock.encode(System.currentTimeMillis()), 0);
    }

    /**
     * Create a new key-value pair with TTL.
     *
     * @param value     the value bytes
     * @param ttlMillis time-to-live in milliseconds (0 for no expiration)
     */
    public KeyValuePair(byte[] value, long ttlMillis) {
        this(value, HybridLogicalClock.encode(System.currentTimeMillis()),
             ttlMillis > 0 ? System.currentTimeMillis() + ttlMillis : 0);
    }

    /**
     * Create a key-value pair with explicit timestamp and expiration.
     * Used for replication and conflict resolution.
     *
     * @param value     the value bytes
     * @param timestamp the creation timestamp
     * @param expiresAt the expiration timestamp (0 for no expiration)
     */
    public KeyValuePair(byte[] value, long timestamp, long expiresAt) {
        this.value = value != null ? Arrays.copyOf(value, value.length) : null;
        this.timestamp = timestamp;
        this.expiresAt = expiresAt;
    }

    /**
     * Get a copy of the value bytes.
     *
     * @return copy of the value
     */
    public byte[] getValue() {
        return value != null ? Arrays.copyOf(value, value.length) : null;
    }

    /**
     * Get the raw value bytes without copying.
     * Use with caution - do not modify the returned array.
     *
     * @return the internal value array
     */
    public byte[] getValueUnsafe() {
        return value;
    }

    /**
     * Get the creation timestamp.
     *
     * @return timestamp in milliseconds since epoch
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Get the expiration timestamp.
     *
     * @return expiration timestamp, or 0 if no expiration
     */
    public long getExpiresAt() {
        return expiresAt;
    }

    /**
     * Check if this entry has expired.
     *
     * @return true if expired
     */
    public boolean isExpired() {
        return expiresAt > 0 && System.currentTimeMillis() > expiresAt;
    }

    /**
     * Check if this entry is a tombstone (deleted value marker).
     * Tombstones are represented as entries with null values.
     *
     * @return true if this is a tombstone
     */
    public boolean isTombstone() {
        return value == null;
    }

    /**
     * Check if this entry has a TTL set.
     *
     * @return true if TTL is set
     */
    public boolean hasTtl() {
        return expiresAt > 0;
    }

    /**
     * Get remaining TTL in milliseconds.
     *
     * @return remaining TTL, or -1 if no TTL, or 0 if expired
     */
    public long getRemainingTtl() {
        if (expiresAt == 0) {
            return -1;
        }
        long remaining = expiresAt - System.currentTimeMillis();
        return Math.max(0, remaining);
    }

    /**
     * Whether this version wins over the other under last-write-wins ({@link #compareVersions}).
     *
     * @param other the current entry, or null if there is none
     */
    public boolean isNewerThan(KeyValuePair other) {
        return other == null || compareVersions(timestamp, value, other.timestamp, other.value) > 0;
    }

    /**
     * The total order of versions used by last-write-wins: the higher timestamp wins; on equal
     * timestamps a tombstone wins (as in Cassandra), then the greater value. Two coordinators can
     * issue the same hybrid-clock timestamp for concurrent writes, so ties must be broken the same
     * way on every node, or replicas that applied the writes in different orders would disagree
     * forever.
     *
     * @return negative, zero or positive as version 1 is older than, the same as, or newer than version 2
     */
    public static int compareVersions(long timestamp1, byte[] value1, long timestamp2, byte[] value2) {
        if (timestamp1 != timestamp2) {
            return Long.compare(timestamp1, timestamp2);
        }
        if (value1 == null || value2 == null) {
            return value1 == value2 ? 0 : (value1 == null ? 1 : -1);
        }
        return Arrays.compare(value1, value2);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyValuePair that = (KeyValuePair) o;
        return timestamp == that.timestamp &&
               expiresAt == that.expiresAt &&
               Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(timestamp, expiresAt);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }

    @Override
    public String toString() {
        return "KeyValuePair{" +
               "valueLength=" + (value != null ? value.length : -1) +
               ", timestamp=" + timestamp +
               ", expiresAt=" + expiresAt +
               ", expired=" + isExpired() +
               '}';
    }
}
