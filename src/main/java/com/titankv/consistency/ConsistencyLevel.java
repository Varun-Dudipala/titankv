package com.titankv.consistency;

/**
 * How many of a key's replicas must answer before a read or write succeeds, as in Cassandra.
 * With replication factor N, QUORUM is N/2 + 1, so QUORUM reads and QUORUM writes always share
 * at least one replica (R + W > N).
 */
public enum ConsistencyLevel {

    /** One replica: lowest latency and highest availability; reads may be stale. */
    ONE,

    /** A majority of replicas: tolerates a minority down; QUORUM reads see QUORUM writes. */
    QUORUM,

    /** Every replica: fails if any replica is down. */
    ALL;

    /**
     * @param replicationFactor the number of replicas a key has
     * @return how many of them must answer
     */
    public int getRequired(int replicationFactor) {
        switch (this) {
            case ONE:
                return 1;
            case QUORUM:
                return replicationFactor / 2 + 1;
            default:
                return replicationFactor;
        }
    }

    /**
     * @return whether the level can still be met with this many replicas down
     */
    public boolean canTolerate(int replicationFactor, int failures) {
        return replicationFactor - failures >= getRequired(replicationFactor);
    }

    /**
     * @return the most replicas that can be down while the level can still be met
     */
    public int maxTolerableFailures(int replicationFactor) {
        return replicationFactor - getRequired(replicationFactor);
    }
}
