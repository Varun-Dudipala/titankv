package com.titankv.consistency;

import com.titankv.TitanKVClient;
import com.titankv.client.ClientConfig;
import com.titankv.cluster.ClusterManager;
import com.titankv.cluster.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages replication of data across nodes with tunable consistency.
 */
public class ReplicationManager {

    private static final Logger logger = LoggerFactory.getLogger(ReplicationManager.class);

    private static final int DEFAULT_REPLICATION_FACTOR = 3;
    private static final long DEFAULT_TIMEOUT_MS = 5000;

    private final ClusterManager clusterManager;
    private final int replicationFactor;
    private final long timeoutMs;
    private final ExecutorService executor;
    private final Map<String, TitanKVClient> nodeClients;

    /**
     * Create a replication manager with default settings.
     *
     * @param clusterManager the cluster manager
     */
    public ReplicationManager(ClusterManager clusterManager) {
        this(clusterManager, DEFAULT_REPLICATION_FACTOR, DEFAULT_TIMEOUT_MS);
    }

    /**
     * Create a replication manager with custom settings.
     *
     * @param clusterManager    the cluster manager
     * @param replicationFactor number of replicas per key
     * @param timeoutMs         timeout for replica operations
     */
    public ReplicationManager(ClusterManager clusterManager, int replicationFactor, long timeoutMs) {
        this.clusterManager = clusterManager;
        this.replicationFactor = replicationFactor;
        this.timeoutMs = timeoutMs;
        this.executor = Executors.newFixedThreadPool(16, r -> {
            Thread t = new Thread(r, "replication-worker");
            t.setDaemon(true);
            return t;
        });
        this.nodeClients = new ConcurrentHashMap<>();
    }

    /**
     * Write to replicas with the specified consistency level.
     *
     * @param key         the key to write
     * @param value       the value to write
     * @param consistency the consistency level required
     * @return CompletableFuture that completes when consistency is met
     */
    public CompletableFuture<Boolean> write(String key, byte[] value, ConsistencyLevel consistency) {
        List<Node> replicas = clusterManager.getNodesForKey(key, replicationFactor);
        int required = consistency.getRequired(replicas.size());

        if (replicas.size() < required) {
            return CompletableFuture.failedFuture(
                new ConsistencyException("Not enough replicas available", consistency, required, replicas.size())
            );
        }

        CompletableFuture<Boolean> result = new CompletableFuture<>();
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        int totalReplicas = replicas.size();

        for (Node replica : replicas) {
            executor.submit(() -> {
                try {
                    writeToNode(replica, key, value);
                    int successes = successCount.incrementAndGet();
                    if (successes >= required && !result.isDone()) {
                        result.complete(true);
                    }
                } catch (Exception e) {
                    logger.warn("Write to {} failed: {}", replica.getId(), e.getMessage());
                    int failures = failureCount.incrementAndGet();
                    if (failures > (totalReplicas - required) && !result.isDone()) {
                        result.completeExceptionally(
                            new ConsistencyException("Cannot meet consistency level",
                                consistency, required, successCount.get())
                        );
                    }
                }
            });
        }

        // Add timeout
        scheduleTimeout(result, consistency, required);

        return result;
    }

    /**
     * Read from replicas with the specified consistency level.
     *
     * @param key         the key to read
     * @param consistency the consistency level required
     * @return CompletableFuture with the value
     */
    public CompletableFuture<Optional<byte[]>> read(String key, ConsistencyLevel consistency) {
        List<Node> replicas = clusterManager.getNodesForKey(key, replicationFactor);
        int required = consistency.getRequired(replicas.size());

        if (replicas.size() < required) {
            return CompletableFuture.failedFuture(
                new ConsistencyException("Not enough replicas available", consistency, required, replicas.size())
            );
        }

        // For ONE, just read from primary
        if (consistency == ConsistencyLevel.ONE && !replicas.isEmpty()) {
            return readFromNode(replicas.get(0), key);
        }

        // For QUORUM/ALL, read from multiple and compare
        return readWithQuorum(key, replicas, required);
    }

    /**
     * Read from multiple replicas and return the most recent value.
     */
    private CompletableFuture<Optional<byte[]>> readWithQuorum(
            String key, List<Node> replicas, int required) {

        CompletableFuture<Optional<byte[]>> result = new CompletableFuture<>();
        List<byte[]> values = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger responseCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);

        for (Node replica : replicas) {
            executor.submit(() -> {
                try {
                    Optional<byte[]> value = readFromNode(replica, key).get(timeoutMs, TimeUnit.MILLISECONDS);
                    value.ifPresent(values::add);

                    int responses = responseCount.incrementAndGet();
                    if (responses >= required && !result.isDone()) {
                        // Return the value (in production, would compare and pick newest)
                        if (values.isEmpty()) {
                            result.complete(Optional.empty());
                        } else {
                            result.complete(Optional.of(values.get(0)));
                        }
                    }
                } catch (Exception e) {
                    logger.warn("Read from {} failed: {}", replica.getId(), e.getMessage());
                    int failures = failureCount.incrementAndGet();
                    if (failures > (replicas.size() - required) && !result.isDone()) {
                        result.completeExceptionally(
                            new ConsistencyException("Cannot meet consistency level",
                                ConsistencyLevel.QUORUM, required, responseCount.get())
                        );
                    }
                }
            });
        }

        return result;
    }

    /**
     * Delete from replicas with the specified consistency level.
     */
    public CompletableFuture<Boolean> delete(String key, ConsistencyLevel consistency) {
        List<Node> replicas = clusterManager.getNodesForKey(key, replicationFactor);
        int required = consistency.getRequired(replicas.size());

        if (replicas.size() < required) {
            return CompletableFuture.failedFuture(
                new ConsistencyException("Not enough replicas available", consistency, required, replicas.size())
            );
        }

        CompletableFuture<Boolean> result = new CompletableFuture<>();
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        int totalReplicas = replicas.size();

        for (Node replica : replicas) {
            executor.submit(() -> {
                try {
                    deleteFromNode(replica, key);
                    int successes = successCount.incrementAndGet();
                    if (successes >= required && !result.isDone()) {
                        result.complete(true);
                    }
                } catch (Exception e) {
                    logger.warn("Delete from {} failed: {}", replica.getId(), e.getMessage());
                    int failures = failureCount.incrementAndGet();
                    if (failures > (totalReplicas - required) && !result.isDone()) {
                        result.completeExceptionally(
                            new ConsistencyException("Cannot meet consistency level",
                                consistency, required, successCount.get())
                        );
                    }
                }
            });
        }

        scheduleTimeout(result, consistency, required);

        return result;
    }

    private void writeToNode(Node node, String key, byte[] value) throws IOException {
        TitanKVClient client = getClient(node);
        client.put(key, value);
    }

    private CompletableFuture<Optional<byte[]>> readFromNode(Node node, String key) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                TitanKVClient client = getClient(node);
                return client.get(key);
            } catch (IOException e) {
                throw new CompletionException(e);
            }
        }, executor);
    }

    private void deleteFromNode(Node node, String key) throws IOException {
        TitanKVClient client = getClient(node);
        client.delete(key);
    }

    private TitanKVClient getClient(Node node) {
        return nodeClients.computeIfAbsent(node.getAddress(), addr -> {
            ClientConfig config = ClientConfig.builder()
                .connectTimeoutMs((int) timeoutMs)
                .readTimeoutMs((int) timeoutMs)
                .retryOnFailure(false)
                .build();
            return new TitanKVClient(config, addr);
        });
    }

    private <T> void scheduleTimeout(CompletableFuture<T> future, ConsistencyLevel level, int required) {
        executor.submit(() -> {
            try {
                Thread.sleep(timeoutMs);
                if (!future.isDone()) {
                    future.completeExceptionally(
                        new ConsistencyException("Timeout waiting for consistency",
                            level, required, 0)
                    );
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    /**
     * Get the replication factor.
     */
    public int getReplicationFactor() {
        return replicationFactor;
    }

    /**
     * Shutdown the replication manager.
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

        // Close all node clients
        for (TitanKVClient client : nodeClients.values()) {
            client.close();
        }
        nodeClients.clear();

        logger.info("Replication manager shutdown complete");
    }
}
