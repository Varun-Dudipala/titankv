package com.titankv.cluster;

import com.titankv.TitanKVClient;
import com.titankv.client.ClientConfig;
import com.titankv.core.InMemoryStore;
import com.titankv.core.KeyValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Handles data rebalancing when cluster membership changes.
 * 
 * When a new node joins:
 * - Existing nodes transfer keys that now belong to the new node
 * 
 * When a node leaves (gracefully):
 * - The leaving node transfers its data to successor nodes
 * 
 * Rebalancing is done in the background without blocking normal operations.
 */
public class DataRebalancer {

    private static final Logger logger = LoggerFactory.getLogger(DataRebalancer.class);

    private static final int BATCH_SIZE = 100;
    private static final int MAX_CONCURRENT_TRANSFERS = 4;
    private static final long TRANSFER_TIMEOUT_MS = 30_000;

    private final ClusterManager clusterManager;
    private final InMemoryStore store;
    private final int replicationFactor;
    private final ExecutorService transferPool;
    private final Map<String, TitanKVClient> nodeClients;
    private final String authToken;
    private volatile boolean running;

    // Track rebalancing state
    private final AtomicInteger activeTransfers = new AtomicInteger(0);
    private volatile boolean rebalanceInProgress = false;

    public DataRebalancer(ClusterManager clusterManager, InMemoryStore store, int replicationFactor) {
        this.clusterManager = clusterManager;
        this.store = store;
        this.replicationFactor = replicationFactor;
        this.nodeClients = new ConcurrentHashMap<>();
        this.authToken = readAuthToken();
        this.running = false;

        this.transferPool = new ThreadPoolExecutor(
            MAX_CONCURRENT_TRANSFERS,
            MAX_CONCURRENT_TRANSFERS,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(1000),
            r -> {
                Thread t = new Thread(r, "data-rebalancer");
                t.setDaemon(true);
                return t;
            },
            new ThreadPoolExecutor.CallerRunsPolicy()
        );

        // Register for cluster events
        clusterManager.addEventListener(this::handleClusterEvent);
    }

    /**
     * Start the data rebalancer.
     */
    public void start() {
        if (running) {
            return;
        }
        running = true;
        logger.info("Data rebalancer started");
    }

    /**
     * Stop the data rebalancer.
     */
    public void stop() {
        if (!running) {
            return;
        }
        running = false;

        transferPool.shutdown();
        try {
            if (!transferPool.awaitTermination(30, TimeUnit.SECONDS)) {
                transferPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            transferPool.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Close all clients
        for (TitanKVClient client : nodeClients.values()) {
            client.close();
        }
        nodeClients.clear();

        logger.info("Data rebalancer stopped");
    }

    private void handleClusterEvent(ClusterManager.ClusterEvent event) {
        if (!running) {
            return;
        }

        switch (event.getType()) {
            case NODE_JOINED:
            case NODE_RECOVERED:
                // New/recovered node - transfer data that should belong to it
                transferPool.submit(() -> handleNodeJoin(event.getNode()));
                break;

            case NODE_LEFT:
                // Node leaving gracefully - data should be redistributed
                // This is handled by the leaving node itself
                break;

            case NODE_DEAD:
                // Node failed - hints are used for writes that couldn't be delivered
                // Read repair will fix reads
                logger.info("Node {} marked dead, relying on hints and read repair", 
                    event.getNode().getId());
                break;

            default:
                break;
        }
    }

    /**
     * Handle a new node joining the cluster.
     * Transfer keys that now belong to this node.
     */
    private void handleNodeJoin(Node newNode) {
        if (newNode.equals(clusterManager.getLocalNode())) {
            // We are the new node - request data from other nodes
            requestDataFromCluster();
            return;
        }

        logger.info("Starting data transfer to new node {}", newNode.getId());
        rebalanceInProgress = true;

        try {
            Set<String> keysToTransfer = findKeysForNode(newNode);
            if (keysToTransfer.isEmpty()) {
                logger.info("No keys to transfer to node {}", newNode.getId());
                return;
            }

            logger.info("Transferring {} keys to node {}", keysToTransfer.size(), newNode.getId());
            transferKeysToNode(newNode, keysToTransfer);

        } catch (Exception e) {
            logger.error("Error during data transfer to node {}: {}", newNode.getId(), e.getMessage());
        } finally {
            rebalanceInProgress = false;
        }
    }

    /**
     * Find keys that should be transferred to a specific node based on consistent hashing.
     */
    private Set<String> findKeysForNode(Node targetNode) {
        Set<String> result = new HashSet<>();
        Node localNode = clusterManager.getLocalNode();

        for (String key : store.keys()) {
            List<Node> owners = clusterManager.getNodesForKey(key, replicationFactor);
            
            // Check if target node should own this key
            boolean targetShouldOwn = owners.stream()
                .anyMatch(n -> n.getId().equals(targetNode.getId()));
            
            // Check if we currently have this key (we should be in the owner list)
            boolean weOwn = owners.stream()
                .anyMatch(n -> n.getId().equals(localNode.getId()));

            // Transfer if: target should own, and we currently have it
            if (targetShouldOwn && weOwn) {
                result.add(key);
            }
        }

        return result;
    }

    /**
     * Transfer a set of keys to a target node.
     */
    private void transferKeysToNode(Node targetNode, Set<String> keys) {
        TitanKVClient client = getClient(targetNode);
        if (client == null) {
            logger.error("Cannot get client for node {}", targetNode.getId());
            return;
        }

        activeTransfers.incrementAndGet();
        int transferred = 0;
        int failed = 0;

        try {
            List<String> batch = new ArrayList<>(BATCH_SIZE);
            
            for (String key : keys) {
                batch.add(key);
                
                if (batch.size() >= BATCH_SIZE) {
                    int batchResult = transferBatch(client, batch);
                    transferred += batchResult;
                    failed += batch.size() - batchResult;
                    batch.clear();
                }
            }

            // Transfer remaining
            if (!batch.isEmpty()) {
                int batchResult = transferBatch(client, batch);
                transferred += batchResult;
                failed += batch.size() - batchResult;
            }

            logger.info("Transfer to {} complete: {} transferred, {} failed",
                targetNode.getId(), transferred, failed);

        } finally {
            activeTransfers.decrementAndGet();
        }
    }

    private int transferBatch(TitanKVClient client, List<String> keys) {
        int success = 0;
        
        for (String key : keys) {
            try {
                Optional<KeyValuePair> entry = store.getRaw(key);
                if (entry.isPresent()) {
                    KeyValuePair kv = entry.get();
                    if (kv.isTombstone()) {
                        client.deleteInternal(key, kv.getTimestamp(), kv.getExpiresAt());
                    } else {
                        client.putInternal(key, kv.getValueUnsafe(), kv.getTimestamp(), kv.getExpiresAt());
                    }
                    success++;
                }
            } catch (IOException e) {
                logger.warn("Failed to transfer key {} : {}", key, e.getMessage());
            }
        }
        
        return success;
    }

    /**
     * Request data from other nodes when we join the cluster.
     * This is called when the local node is the new node.
     */
    private void requestDataFromCluster() {
        logger.info("Requesting data from cluster as new node");
        
        // We'll receive data via PUT_INTERNAL from other nodes
        // Just log that we're ready to receive
        logger.info("Ready to receive data transfers from existing nodes");
    }

    /**
     * Prepare to leave the cluster gracefully.
     * Transfer all local data to successor nodes.
     */
    public CompletableFuture<Void> prepareToLeave() {
        logger.info("Preparing to leave cluster, transferring data to successors");
        rebalanceInProgress = true;

        return CompletableFuture.runAsync(() -> {
            try {
                Set<String> allKeys = store.keys();
                logger.info("Transferring {} keys before leaving", allKeys.size());

                for (String key : allKeys) {
                    try {
                        // Get successor nodes (excluding ourselves)
                        List<Node> successors = clusterManager.getNodesForKey(key, replicationFactor + 1);
                        successors.removeIf(n -> n.equals(clusterManager.getLocalNode()));

                        if (successors.isEmpty()) {
                            logger.warn("No successors found for key {}", key);
                            continue;
                        }

                        // Transfer to first available successor
                        Optional<KeyValuePair> entry = store.getRaw(key);
                        if (entry.isPresent()) {
                            KeyValuePair kv = entry.get();
                            boolean transferred = false;

                            for (Node successor : successors) {
                                if (!successor.isAvailable()) {
                                    continue;
                                }
                                try {
                                    TitanKVClient client = getClient(successor);
                                    if (kv.isTombstone()) {
                                        client.deleteInternal(key, kv.getTimestamp(), kv.getExpiresAt());
                                    } else {
                                        client.putInternal(key, kv.getValueUnsafe(), kv.getTimestamp(), kv.getExpiresAt());
                                    }
                                    transferred = true;
                                    break;
                                } catch (IOException e) {
                                    logger.warn("Failed to transfer key {} to {}: {}", 
                                        key, successor.getId(), e.getMessage());
                                }
                            }

                            if (!transferred) {
                                logger.error("Failed to transfer key {} to any successor", key);
                            }
                        }
                    } catch (Exception e) {
                        logger.error("Error transferring key {}: {}", key, e.getMessage());
                    }
                }

                logger.info("Data transfer complete, ready to leave cluster");

            } finally {
                rebalanceInProgress = false;
            }
        }, transferPool);
    }

    private TitanKVClient getClient(Node node) {
        return nodeClients.computeIfAbsent(node.getAddress(), addr -> {
            ClientConfig config = ClientConfig.builder()
                .connectTimeoutMs(5000)
                .readTimeoutMs((int) TRANSFER_TIMEOUT_MS)
                .retryOnFailure(false)
                .authToken(authToken)
                .build();
            return new TitanKVClient(config, addr);
        });
    }

    private static String readAuthToken() {
        String value = System.getenv("TITANKV_INTERNAL_TOKEN");
        if (value == null || value.isEmpty()) {
            value = System.getProperty("titankv.internal.token");
        }
        if (value == null || value.isEmpty()) {
            value = System.getenv("TITANKV_CLUSTER_SECRET");
        }
        if (value == null || value.isEmpty()) {
            value = System.getProperty("titankv.cluster.secret");
        }
        return value;
    }

    /**
     * Check if rebalancing is in progress.
     */
    public boolean isRebalanceInProgress() {
        return rebalanceInProgress;
    }

    /**
     * Get the number of active transfers.
     */
    public int getActiveTransfers() {
        return activeTransfers.get();
    }
}
