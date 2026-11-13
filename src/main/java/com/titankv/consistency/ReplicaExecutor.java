package com.titankv.consistency;

import com.titankv.cluster.Node;

import java.util.concurrent.RejectedExecutionException;

/**
 * Runs blocking calls to replicas off the caller's thread. Implementations keep each replica's
 * calls apart, so calls stuck on a slow or unreachable replica cannot use up the threads that
 * calls to healthy replicas need.
 */
@FunctionalInterface
public interface ReplicaExecutor {

    /**
     * @throws RejectedExecutionException if the replica already has too many calls waiting, or
     *                                    the executor is shut down; the caller treats the call
     *                                    as failed
     */
    void execute(Node replica, Runnable task);
}
