package com.titankv.consistency;

import com.titankv.TitanKVClient;
import com.titankv.client.ClientConfig;
import com.titankv.cluster.ClusterManager;
import com.titankv.cluster.Node;
import com.titankv.core.KeyValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * Handles read repair to fix stale data across replicas.
 * When a read detects inconsistency, this repairs the stale replicas.
 */
public class ReadRepairHandler {

    private static final Logger logger = LoggerFactory.getLogger(ReadRepairHandler.class);

    private final ClusterManager clusterManager;
    private final int replicationFactor;
    private final ExecutorService executor;
    private final Map<String, TitanKVClient> nodeClients;

    /**
     * Create a read repair handler.
     *
     * @param clusterManager    the cluster manager
     * @param replicationFactor the replication factor
     */
    public ReadRepairHandler(ClusterManager clusterManager, int replicationFactor) {
        this.clusterManager = clusterManager;
        this.replicationFactor = replicationFactor;
        this.executor = Executors.newFixedThreadPool(4, r -> {
            Thread t = new Thread(r, "read-repair");
            t.setDaemon(true);
            return t;
        });
        this.nodeClients = new ConcurrentHashMap<>();
    }

    /**
     * Read repair result containing the value and nodes that were repaired.
     */
    public static class RepairResult {
        private final byte[] value;
        private final long timestamp;
        private final List<Node> repairedNodes;
        private final boolean repairNeeded;

        public RepairResult(byte[] value, long timestamp, List<Node> repairedNodes, boolean repairNeeded) {
            this.value = value;
            this.timestamp = timestamp;
            this.repairedNodes = repairedNodes;
            this.repairNeeded = repairNeeded;
        }

        public byte[] getValue() {
            return value;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public List<Node> getRepairedNodes() {
            return repairedNodes;
        }

        public boolean isRepairNeeded() {
            return repairNeeded;
        }
    }

    /**
     * Read with repair capability.
     * Reads from all replicas, compares values, and repairs stale nodes.
     *
     * @param key the key to read
     * @return the repair result with the most recent value
     */
    public CompletableFuture<RepairResult> readWithRepair(String key) {
        List<Node> replicas = clusterManager.getNodesForKey(key, replicationFactor);

        if (replicas.isEmpty()) {
            return CompletableFuture.failedFuture(
                new ConsistencyException("No replicas available for key: " + key)
            );
        }

        // Read from all replicas
        List<CompletableFuture<NodeValue>> futures = new ArrayList<>();
        for (Node replica : replicas) {
            futures.add(readFromReplica(replica, key));
        }

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .thenApply(v -> {
                List<NodeValue> results = new ArrayList<>();
                for (CompletableFuture<NodeValue> future : futures) {
                    try {
                        NodeValue nv = future.get();
                        if (nv != null) {
                            results.add(nv);
                        }
                    } catch (Exception e) {
                        logger.debug("Failed to get result from replica: {}", e.getMessage());
                    }
                }

                if (results.isEmpty()) {
                    return new RepairResult(null, 0, Collections.emptyList(), false);
                }

                // Find the newest value
                NodeValue newest = findNewest(results);
                if (newest == null || newest.value == null) {
                    return new RepairResult(null, 0, Collections.emptyList(), false);
                }

                // Find nodes that need repair
                List<Node> staleNodes = findStaleNodes(results, newest);

                if (!staleNodes.isEmpty()) {
                    // Asynchronously repair stale nodes
                    repairNodes(key, newest.value, staleNodes);
                    logger.info("Read repair triggered for key {} on {} nodes",
                        key, staleNodes.size());
                }

                return new RepairResult(
                    newest.value,
                    newest.timestamp,
                    staleNodes,
                    !staleNodes.isEmpty()
                );
            });
    }

    /**
     * Force repair on all replicas for a key.
     *
     * @param key   the key
     * @param value the correct value
     * @return future that completes when repair is done
     */
    public CompletableFuture<Integer> forceRepair(String key, byte[] value) {
        List<Node> replicas = clusterManager.getNodesForKey(key, replicationFactor);
        return repairNodesAsync(key, value, replicas);
    }

    private CompletableFuture<NodeValue> readFromReplica(Node node, String key) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                TitanKVClient client = getClient(node);
                Optional<byte[]> value = client.get(key);
                if (value.isPresent()) {
                    // In a real implementation, would get the timestamp from the server
                    return new NodeValue(node, value.get(), System.currentTimeMillis());
                }
                return new NodeValue(node, null, 0);
            } catch (IOException e) {
                logger.debug("Read from {} failed: {}", node.getId(), e.getMessage());
                return null;
            }
        }, executor);
    }

    private NodeValue findNewest(List<NodeValue> results) {
        NodeValue newest = null;
        for (NodeValue nv : results) {
            if (nv != null && nv.value != null) {
                if (newest == null || nv.timestamp > newest.timestamp) {
                    newest = nv;
                }
            }
        }
        return newest;
    }

    private List<Node> findStaleNodes(List<NodeValue> results, NodeValue newest) {
        List<Node> stale = new ArrayList<>();
        for (NodeValue nv : results) {
            if (nv != null && nv.node != null && !nv.node.equals(newest.node)) {
                if (nv.value == null || !Arrays.equals(nv.value, newest.value)) {
                    stale.add(nv.node);
                }
            }
        }
        return stale;
    }

    private void repairNodes(String key, byte[] value, List<Node> nodes) {
        executor.submit(() -> {
            for (Node node : nodes) {
                try {
                    TitanKVClient client = getClient(node);
                    client.put(key, value);
                    logger.debug("Repaired key {} on node {}", key, node.getId());
                } catch (IOException e) {
                    logger.warn("Failed to repair {} on {}: {}", key, node.getId(), e.getMessage());
                }
            }
        });
    }

    private CompletableFuture<Integer> repairNodesAsync(String key, byte[] value, List<Node> nodes) {
        List<CompletableFuture<Boolean>> futures = new ArrayList<>();

        for (Node node : nodes) {
            futures.add(CompletableFuture.supplyAsync(() -> {
                try {
                    TitanKVClient client = getClient(node);
                    client.put(key, value);
                    return true;
                } catch (IOException e) {
                    logger.warn("Failed to repair {} on {}: {}", key, node.getId(), e.getMessage());
                    return false;
                }
            }, executor));
        }

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .thenApply(v -> {
                int count = 0;
                for (CompletableFuture<Boolean> f : futures) {
                    try {
                        if (f.get()) count++;
                    } catch (Exception e) {
                        // Ignore
                    }
                }
                return count;
            });
    }

    private TitanKVClient getClient(Node node) {
        return nodeClients.computeIfAbsent(node.getAddress(), addr -> {
            ClientConfig config = ClientConfig.builder()
                .connectTimeoutMs(5000)
                .readTimeoutMs(5000)
                .retryOnFailure(false)
                .build();
            return new TitanKVClient(config, addr);
        });
    }

    /**
     * Shutdown the read repair handler.
     */
    public void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        for (TitanKVClient client : nodeClients.values()) {
            client.close();
        }
        nodeClients.clear();
    }

    /**
     * Internal class to track node values.
     */
    private static class NodeValue {
        final Node node;
        final byte[] value;
        final long timestamp;

        NodeValue(Node node, byte[] value, long timestamp) {
            this.node = node;
            this.value = value;
            this.timestamp = timestamp;
        }
    }
}
