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
     * Check if this entry is a value whose time-to-live has run out. A tombstone never expires;
     * it is purged once it is older than the tombstone grace period.
     *
     * @return true if expired
     */
    public boolean isExpired() {
        return value != null && expiresAt > 0 && System.currentTimeMillis() > expiresAt;
    }

    /**
     * The tombstone that takes this entry's place once it has expired: the same version without
     * the value. An expired entry must keep its version rather than vanish, or an older version
     * still held by a replica that missed this write would win and come back. The expiry time is
     * kept as the time the key stopped existing (see {@link #deletedAtMillis()}).
     */
    public KeyValuePair toTombstone() {
        return new KeyValuePair(null, timestamp, expiresAt);
    }

    /**
     * When this tombstone's key stopped existing, in epoch milliseconds: the expiry time for a
     * value that expired, otherwise the time of the delete. The tombstone grace period counts
     * from here.
     */
    public long deletedAtMillis() {
        return expiresAt > 0 ? expiresAt : HybridLogicalClock.physicalMillis(timestamp);
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
        return other == null
                || compareVersions(timestamp, value, expiresAt, other.timestamp, other.value, other.expiresAt) > 0;
    }

    /**
     * The order replicas store by: {@link #compareVersions(long, byte[], long, byte[])}, and
     * between entries that are the same write, the later expiry. A write with a TTL that is sent
     * twice with one version (a failover, or a retry on a stale connection) gets its expiry from
     * each coordinator's clock, so replicas can hold the same version with different expiry
     * times. Without an order between those, neither replaced the other, while anti-entropy saw
     * them differ and exchanged them on every round without ever converging.
     */
    public static int compareVersions(long timestamp1, byte[] value1, long expiresAt1,
            long timestamp2, byte[] value2, long expiresAt2) {
        int byVersion = compareVersions(timestamp1, value1, timestamp2, value2);
        return byVersion != 0 ? byVersion : Long.compare(expiresAt1, expiresAt2);
    }

    /**
     * The total order of versions used by last-write-wins: the higher timestamp wins; on equal
     * timestamps a tombstone wins (as in Cassandra), then the greater value. Two coordinators can
     * issue the same hybrid-clock timestamp for concurrent writes, so ties must be broken the same
     * way on every node, or replicas that applied the writes in different orders would disagree
     * forever.
     *
     * This compares writes. Whether one write supersedes another is decided here; two copies of
     * the same write that differ only in expiry compare as equal.
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
