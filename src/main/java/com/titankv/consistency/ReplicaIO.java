package com.titankv.consistency;

import com.titankv.cluster.Node;

import java.io.IOException;
import java.util.Optional;

/**
 * Reads and writes a single replica's local copy of a key, without further replication.
 */
public interface ReplicaIO {

    /**
     * @return the replica's raw entry (tombstones included, value null), or empty if it has none
     */
    Optional<ReplicationManager.ReadResult> read(Node replica, String key) throws IOException;

    /**
     * Write a versioned value (null for a tombstone); the replica keeps whichever version is newer.
     */
    void write(Node replica, String key, byte[] value, long timestamp, long expiresAt) throws IOException;

    /**
     * Whether calls for this replica are served in-process (no network), so they are cheap enough
     * to run on the caller's thread.
     */
    default boolean isInProcess(Node replica) {
        return false;
    }
}
