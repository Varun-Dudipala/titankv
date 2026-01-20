package com.titankv.cluster;

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

    private GossipProtocol gossipProtocol;
    private volatile boolean running;

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
        String secret = System.getenv("TITANKV_CLUSTER_SECRET");
        if (secret == null || secret.isEmpty()) {
            secret = System.getProperty("titankv.cluster.secret");
        }

        // Check if dev mode is explicitly enabled
        boolean devMode = "true".equalsIgnoreCase(System.getenv("TITANKV_DEV_MODE"))
            || "true".equalsIgnoreCase(System.getProperty("titankv.dev.mode"));

        // In production mode, require a secret
        if (!devMode && (secret == null || secret.isEmpty())) {
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

        // Start gossip protocol with authentication
        gossipProtocol = new GossipProtocol(localNode, this, clusterSecret);
        gossipProtocol.start();

        // Join cluster via seed nodes
        // Note: Don't call addNode() here - we don't know the real node IDs yet.
        // The seed nodes will respond with JOIN messages containing their real IDs,
        // or send us membership lists with all known nodes.
        if (seedNodes != null && !seedNodes.isEmpty()) {
            for (String seed : seedNodes.split(",")) {
                String trimmed = seed.trim();
                if (!trimmed.isEmpty() && !trimmed.equals(localNode.getAddress())) {
                    Node seedNode = Node.fromAddress(trimmed);
                    gossipProtocol.sendJoin(seedNode);
                }
            }
        }

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
        nodes.put(node.getId(), node);
        hashRing.addNode(node);

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
            hashRing.removeNode(removed);
            fireEvent(new ClusterEvent(ClusterEvent.Type.NODE_LEFT, removed));
            logger.info("Node {} left the cluster", node.getId());
        }
    }

    /**
     * Update heartbeat for a node.
     *
     * @param nodeId the node identifier
     */
    public void updateHeartbeat(String nodeId) {
        Node node = nodes.get(nodeId);
        if (node != null) {
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
        long now = System.currentTimeMillis();

        for (Node node : nodes.values()) {
            if (node.equals(localNode)) {
                continue;
            }

            long lastHeartbeat = node.getLastHeartbeat();
            long elapsed = now - lastHeartbeat;

            if (node.getStatus() == Node.Status.ALIVE && elapsed > SUSPECT_THRESHOLD_MS) {
                node.setStatus(Node.Status.SUSPECT);
                fireEvent(new ClusterEvent(ClusterEvent.Type.NODE_SUSPECT, node));
                logger.warn("Node {} is suspect (no heartbeat for {}ms)", node.getId(), elapsed);
            } else if (node.getStatus() == Node.Status.SUSPECT && elapsed > DEAD_THRESHOLD_MS) {
                node.setStatus(Node.Status.DEAD);
                hashRing.removeNode(node);
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
            NODE_RECOVERED
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
