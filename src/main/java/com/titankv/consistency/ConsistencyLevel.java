package com.titankv.consistency;

/**
 * Tunable consistency levels for reads and writes.
 * Similar to Cassandra's consistency model.
 */
public enum ConsistencyLevel {

    /**
     * ONE: Operation succeeds after one node responds.
     * - Fastest latency
     * - Risk of reading stale data
     * - Best for: non-critical data, high throughput requirements
     */
    ONE(1),

    /**
     * QUORUM: Majority of replicas must respond.
     * - Balanced latency and consistency
     * - Formula: (Replication Factor / 2) + 1
     * - Best for: most production workloads
     */
    QUORUM(-1), // Calculated dynamically

    /**
     * ALL: All replicas must respond.
     * - Highest consistency guarantee
     * - Slowest, vulnerable to single node failure
     * - Best for: critical data requiring strong consistency
     */
    ALL(-1); // All replicas

    private final int fixedCount;

    ConsistencyLevel(int fixedCount) {
        this.fixedCount = fixedCount;
    }

    /**
     * Get the number of nodes required for this consistency level.
     *
     * @param replicationFactor the total number of replicas
     * @return number of nodes required
     */
    public int getRequired(int replicationFactor) {
        switch (this) {
            case ONE:
                return 1;
            case QUORUM:
                return (replicationFactor / 2) + 1;
            case ALL:
                return replicationFactor;
            default:
                return fixedCount > 0 ? fixedCount : 1;
        }
    }

    /**
     * Check if this consistency level can tolerate the given number of failures.
     *
     * @param replicationFactor total replicas
     * @param failures          number of failed nodes
     * @return true if the operation can still succeed
     */
    public boolean canTolerate(int replicationFactor, int failures) {
        int required = getRequired(replicationFactor);
        return (replicationFactor - failures) >= required;
    }

    /**
     * Get the maximum number of failures this level can tolerate.
     *
     * @param replicationFactor total replicas
     * @return maximum tolerable failures
     */
    public int maxTolerableFailures(int replicationFactor) {
        return replicationFactor - getRequired(replicationFactor);
    }
}
