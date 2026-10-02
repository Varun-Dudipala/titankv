package com.titankv.cluster;

import com.titankv.util.Env;
import com.titankv.util.HybridLogicalClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * Manages cluster membership, node health, and topology.
 */
public class ClusterManager {

    private static final Logger logger = LoggerFactory.getLogger(ClusterManager.class);

    private static final long SUSPECT_THRESHOLD_MS = 3000;  // 3 missed heartbeats
    private static final long DEAD_THRESHOLD_MS = 10000;    // Confirmed dead

    private final Node localNode;
    private final ConsistentHash hashRing;
    private final Map<String, Node> nodes;
    private final List<Consumer<ClusterEvent>> eventListeners;
    private final ScheduledExecutorService scheduler;
    private final String clusterSecret;
    private final Map<String, Long> departedGenerations = new ConcurrentHashMap<>();
    // Generation of each node that announced a graceful shutdown; its heartbeats from that
    // generation no longer count, so stale gossip about it cannot mark it alive again
    private final Map<String, Long> shutdownGenerations = new ConcurrentHashMap<>();
    private final HybridLogicalClock clock = new HybridLogicalClock();

    private GossipProtocol gossipProtocol;
    private volatile boolean running;
    private volatile boolean seedsConfigured;

    /**
     * Create a new cluster manager.
     *
     * @param localNode the local node
     */
    public ClusterManager(Node localNode) {
        this(localNode, getDefaultClusterSecret());
    }

    /**
     * Create a new cluster manager with explicit cluster secret.
     *
     * @param localNode     the local node
     * @param clusterSecret optional cluster secret for gossip authentication (null to disable)
     */
    public ClusterManager(Node localNode, String clusterSecret) {
        this.localNode = localNode;
        this.clusterSecret = clusterSecret;
        this.hashRing = new ConsistentHash();
        this.nodes = new ConcurrentHashMap<>();
        this.eventListeners = new CopyOnWriteArrayList<>();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "cluster-health");
            t.setDaemon(true);
            return t;
        });
        this.running = false;

        // Add local node to the cluster
        localNode.setStatus(Node.Status.ALIVE);
        nodes.put(localNode.getId(), localNode);
        hashRing.addNode(localNode);

        if (clusterSecret != null && !clusterSecret.isEmpty()) {
            logger.info("Gossip authentication enabled with cluster secret");
        } else {
            logger.warn("Gossip authentication DISABLED (dev mode) - cluster is vulnerable to spoofing attacks. " +
                "This should NEVER be used in production!");
        }
    }

    /**
     * Get cluster secret from environment or system property.
     * Priority: TITANKV_CLUSTER_SECRET env var > titankv.cluster.secret property > null
     *
     * In production mode (default), a secret is required unless TITANKV_DEV_MODE=true.
     * This prevents accidentally deploying clusters without authentication.
     */
    private static String getDefaultClusterSecret() {
        String secret = Env.clusterSecret();
        if (secret == null && !Env.isDevMode()) {
            throw new IllegalStateException(
                "Cluster secret is required for production deployment. " +
                "Set TITANKV_CLUSTER_SECRET environment variable or titankv.cluster.secret property. " +
                "To run in development mode without authentication (UNSAFE), set TITANKV_DEV_MODE=true."
            );
        }
        return secret;
    }

    /**
     * Start the cluster manager.
     *
     * @param seedNodes comma-separated list of seed node addresses
     */
    public void start(String seedNodes) {
        if (running) {
            return;
        }
        running = true;

        gossipProtocol = new GossipProtocol(localNode, this, clusterSecret);

        // Join cluster via seed nodes
        // Note: Don't call addNode() here - we don't know the real node IDs yet.
        // The seed nodes will respond with JOIN messages containing their real IDs,
        // or send us membership lists with all known nodes.
        // Gossip keeps re-sending JOIN to the seeds until it learns about another node,
        // so a lost UDP packet or a seed that starts late does not leave this node isolated.
        List<Node> seeds = new ArrayList<>();
        if (seedNodes != null && !seedNodes.isEmpty()) {
            for (String seed : seedNodes.split(",")) {
                String trimmed = seed.trim();
                if (!trimmed.isEmpty() && !trimmed.equals(localNode.getAddress())) {
                    seeds.add(Node.fromAddress(trimmed));
                }
            }
        }
        seedsConfigured = !seeds.isEmpty();
        gossipProtocol.setSeeds(seeds);
        gossipProtocol.start();

        // Start health check task
        scheduler.scheduleAtFixedRate(this::checkHealth,
            1000, 1000, TimeUnit.MILLISECONDS);

        logger.info("Cluster manager started for node {}", localNode.getId());
    }

    /**
     * Stop the cluster manager.
     */
    public void stop() {
        if (!running) {
            return;
        }
        running = false;

        // Notify cluster we're leaving
        localNode.setStatus(Node.Status.LEAVING);
        if (gossipProtocol != null) {
            gossipProtocol.stop();
        }

        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }

        logger.info("Cluster manager stopped");
    }

    /**
     * Add a node to the cluster.
     *
     * @param node the node to add
     */
    public void addNode(Node node) {
        if (node == null || nodes.containsKey(node.getId())) {
            return;
        }

        // Check if we already have a node at this address (prevent duplicates from seed node handling)
        String nodeAddress = node.getAddress();
        for (Node existing : nodes.values()) {
            if (existing.getAddress().equals(nodeAddress)) {
                logger.debug("Node at {} already exists with ID {}, ignoring duplicate with ID {}", 
                    nodeAddress, existing.getId(), node.getId());
                return;
            }
        }

        if (node.getStatus() == Node.Status.JOINING) {
            node.setStatus(Node.Status.ALIVE);
        }
        node.updateHeartbeat();
        departedGenerations.remove(node.getId());
        nodes.put(node.getId(), node);
        if (node.isAvailable()) {
            hashRing.addNode(node);
        }

        fireEvent(new ClusterEvent(ClusterEvent.Type.NODE_JOINED, node));
        logger.info("Node {} joined the cluster", node.getId());
    }

    /**
     * Remove a node from the cluster.
     *
     * @param node the node to remove
     */
    public void removeNode(Node node) {
        if (node == null || node.equals(localNode)) {
            return;
        }

        Node removed = nodes.remove(node.getId());
        if (removed != null) {
            departedGenerations.put(removed.getId(), removed.getGeneration());
            hashRing.removeNode(removed);
            fireEvent(new ClusterEvent(ClusterEvent.Type.NODE_LEFT, removed));
            logger.info("Node {} left the cluster", node.getId());
        }
    }

    /**
     * Permanently remove a dead node from the cluster (like Cassandra's removenode), on this node
     * and, through gossip, on every other node. Its keys move to other replicas, which
     * anti-entropy then fills in.
     *
     * @return null on success, otherwise why the node cannot be removed
     */
    public String removeDeadNode(String nodeId) {
        Node node = nodes.get(nodeId);
        if (node == null) {
            return "Unknown node " + nodeId;
        }
        if (node.equals(localNode)) {
            return "A node cannot remove itself; stop it to leave the cluster";
        }
        if (node.getStatus() != Node.Status.DEAD) {
            return "Node " + nodeId + " is " + node.getStatus() + "; only DEAD nodes can be removed";
        }
        removeNode(node);
        if (gossipProtocol != null) {
            gossipProtocol.broadcastRemoval(node);
        }
        return null;
    }

    /**
     * Apply a removal gossiped by another node. The generation is remembered even if the node is
     * unknown here, so stale gossip about it is ignored.
     */
    void markRemoved(String nodeId, long generation) {
        Node node = nodes.get(nodeId);
        if (node != null && node.getGeneration() <= generation) {
            removeNode(node);
        }
        departedGenerations.merge(nodeId, generation, Math::max);
    }

    /**
     * Apply a peer's announcement that it is shutting down. It is marked DEAD right away, rather
     * than after the failure detector's 10 seconds, but it stays a member and stays on the ring,
     * as in Cassandra: a shutdown is usually a restart, and taking the node off the ring would
     * hand its keys to nodes that do not have them. Writes for it become hints until it returns
     * (with a newer generation). Removing a node for good is {@link #removeDeadNode}.
     */
    void markShutdown(String nodeId) {
        Node node = nodes.get(nodeId);
        if (node == null || node.equals(localNode)) {
            return;
        }
        shutdownGenerations.merge(nodeId, node.getGeneration(), Math::max);
        if (node.getStatus() != Node.Status.DEAD) {
            node.setStatus(Node.Status.DEAD);
            fireEvent(new ClusterEvent(ClusterEvent.Type.NODE_DEAD, node));
            logger.info("Node {} shut down", nodeId);
        }
    }

    /**
     * Whether a node with this id was removed from the cluster and this generation of it
     * should not be re-added from stale gossip.
     */
    public boolean hasDeparted(String nodeId, long generation) {
        Long departed = departedGenerations.get(nodeId);
        return departed != null && generation <= departed;
    }

    /**
     * Update heartbeat for a node.
     *
     * @param nodeId the node identifier
     */
    public void updateHeartbeat(String nodeId) {
        Node node = nodes.get(nodeId);
        if (node != null) {
            Long shutdownGeneration = shutdownGenerations.get(nodeId);
            if (shutdownGeneration != null && node.getGeneration() <= shutdownGeneration) {
                return; // gossip about a process that has already shut down
            }
            Node.Status oldStatus = node.getStatus();
            node.updateHeartbeat();

            if (oldStatus == Node.Status.SUSPECT || oldStatus == Node.Status.DEAD) {
                node.setStatus(Node.Status.ALIVE);
                if (!hashRing.containsNode(node)) {
                    hashRing.addNode(node);
                }
                fireEvent(new ClusterEvent(ClusterEvent.Type.NODE_RECOVERED, node));
                logger.info("Node {} recovered", nodeId);
            }
        }
    }

    /**
     * Check health of all nodes.
     */
    private void checkHealth() {
        for (Node node : nodes.values()) {
            if (node.equals(localNode)) {
                continue;
            }

            long elapsed = node.getMillisSinceLastHeartbeat();

            if (node.getStatus() == Node.Status.ALIVE && elapsed > SUSPECT_THRESHOLD_MS) {
                node.setStatus(Node.Status.SUSPECT);
                fireEvent(new ClusterEvent(ClusterEvent.Type.NODE_SUSPECT, node));
                logger.warn("Node {} is suspect (no heartbeat for {}ms)", node.getId(), elapsed);
            } else if (node.getStatus() == Node.Status.SUSPECT && elapsed > DEAD_THRESHOLD_MS) {
                // Stays on the ring: its keys keep the same replicas, and writes meant for it
                // are kept as hints until it returns.
                node.setStatus(Node.Status.DEAD);
                fireEvent(new ClusterEvent(ClusterEvent.Type.NODE_DEAD, node));
                logger.error("Node {} is dead (no heartbeat for {}ms)", node.getId(), elapsed);
            }
        }
    }

    /**
     * Get the node responsible for a key.
     *
     * @param key the key
     * @return the node responsible for this key
     */
    public Node getNodeForKey(String key) {
        return hashRing.getNode(key);
    }

    /**
     * Get nodes for replication.
     *
     * @param key   the key
     * @param count number of nodes
     * @return list of nodes
     */
    public List<Node> getNodesForKey(String key, int count) {
        return hashRing.getNodes(key, count);
    }

    /**
     * The key's replicas, including ones that are currently down. Dead nodes stay on the ring
     * until they leave, so this set is stable across failures.
     */
    public List<Node> getReplicasForKey(String key, int count) {
        return hashRing.getReplicas(key, count);
    }

    /**
     * Called by gossip when a known node reports a newer generation, meaning its process restarted
     * and may have lost data that was not on disk.
     */
    public void nodeRestarted(Node node) {
        fireEvent(new ClusterEvent(ClusterEvent.Type.NODE_RESTARTED, node));
        logger.info("Node {} restarted", node.getId());
    }

    /**
     * Get all nodes in the cluster.
     */
    public Collection<Node> getAllNodes() {
        return Collections.unmodifiableCollection(nodes.values());
    }

    /**
     * Get a node by ID.
     */
    public Node getNode(String nodeId) {
        return nodes.get(nodeId);
    }

    /**
     * Get the local node.
     */
    public Node getLocalNode() {
        return localNode;
    }

    /**
     * Get the consistent hash ring.
     */
    public ConsistentHash getHashRing() {
        return hashRing;
    }

    /**
     * Get the number of nodes in the cluster.
     */
    public int getNodeCount() {
        return nodes.size();
    }

    /**
     * Get the number of alive nodes.
     */
    public int getAliveNodeCount() {
        return (int) nodes.values().stream()
            .filter(Node::isAvailable)
            .count();
    }

    /**
     * Add an event listener.
     */
    public void addEventListener(Consumer<ClusterEvent> listener) {
        eventListeners.add(listener);
    }

    /**
     * Remove an event listener.
     */
    public void removeEventListener(Consumer<ClusterEvent> listener) {
        eventListeners.remove(listener);
    }

    private void fireEvent(ClusterEvent event) {
        for (Consumer<ClusterEvent> listener : eventListeners) {
            try {
                listener.accept(event);
            } catch (Exception e) {
                logger.error("Error in event listener", e);
            }
        }
    }

    /**
     * This node's clock for versioning writes.
     */
    public HybridLogicalClock getClock() {
        return clock;
    }

    /**
     * Whether this node may serve client requests. A node started with seeds belongs to a cluster,
     * so until it has found another member it must not act as a one-node cluster: it would accept
     * writes with a single copy and serve reads that miss data held by the rest of the cluster.
     */
    public boolean isReady() {
        return running && (!seedsConfigured || nodes.size() > 1);
    }

    /**
     * Check if the cluster manager is running.
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * Cluster event class.
     */
    public static class ClusterEvent {
        public enum Type {
            NODE_JOINED,
            NODE_LEFT,
            NODE_SUSPECT,
            NODE_DEAD,
            NODE_RECOVERED,
            NODE_RESTARTED
        }

        private final Type type;
        private final Node node;
        private final long timestamp;

        public ClusterEvent(Type type, Node node) {
            this.type = type;
            this.node = node;
            this.timestamp = System.currentTimeMillis();
        }

        public Type getType() {
            return type;
        }

        public Node getNode() {
            return node;
        }

        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public String toString() {
            return "ClusterEvent{type=" + type + ", node=" + node.getId() + "}";
        }
    }
}
