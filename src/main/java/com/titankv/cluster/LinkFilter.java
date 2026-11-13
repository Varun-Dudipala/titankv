package com.titankv.cluster;

import java.io.IOException;

/**
 * Fault injection for tests: decides what happens to the traffic one node sends another, so a
 * test can partition a cluster, cut a link in one direction, or slow it down, as Cassandra's
 * in-JVM tests do with message filters. Every node consults its filter before sending a gossip
 * datagram and before each node-to-node request (replica reads and writes, hints, read repair,
 * anti-entropy, streaming to a joining node). Production nodes keep {@link #NONE}.
 */
public interface LinkFilter {

    /** Lets everything through. */
    LinkFilter NONE = new LinkFilter() {
        @Override
        public boolean allowsGossip(Node from, Node to) {
            return true;
        }

        @Override
        public void beforeRequest(Node from, Node to) {
            // no faults
        }
    };

    /**
     * @return false to drop the datagram, as a partitioned or lossy network would
     */
    boolean allowsGossip(Node from, Node to);

    /**
     * Called on the sending thread before a request to another node. It may sleep to add latency,
     * and throws to fail the request the way an unreachable peer would.
     */
    void beforeRequest(Node from, Node to) throws IOException;

    /**
     * Called after another node has carried out a request and answered. Throwing loses the
     * answer: the sender sees a failure for something that did happen, which is the case that
     * makes retries, hints for data the replica already has, and failover with a reused version
     * occur.
     */
    default void afterRequest(Node from, Node to) throws IOException {
        // no faults
    }

    /**
     * @return false to lose this node's reply to a client request it has already carried out;
     *         the connection is closed instead, as a network failure would leave it
     */
    default boolean deliversClientReply(Node from) {
        return true;
    }
}
