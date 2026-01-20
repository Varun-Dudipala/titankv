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
    private final ScheduledExecutorService timeoutScheduler;
    private final Map<String, TitanKVClient> nodeClients;
    private final ReadRepairHandler readRepairHandler;
    private final String internalAuthToken;

    /**
     * Read result with value metadata.
     */
    public static class ReadResult {
        private final byte[] value;
        private final long timestamp;
        private final long expiresAt;

        public ReadResult(byte[] value, long timestamp, long expiresAt) {
            this.value = value;
            this.timestamp = timestamp;
            this.expiresAt = expiresAt;
        }

        public byte[] getValue() {
            return value;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public long getExpiresAt() {
            return expiresAt;
        }
    }

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

        // Use a bounded pool size to avoid thread explosion under load
        int poolSize = Math.max(16, Runtime.getRuntime().availableProcessors() * 4);
        String envThreads = System.getenv("TITANKV_REPLICATION_THREADS");
        if (envThreads != null && !envThreads.isEmpty()) {
            try {
                poolSize = Integer.parseInt(envThreads);
            } catch (NumberFormatException e) {
                logger.warn("Invalid TITANKV_REPLICATION_THREADS, using default {}", poolSize);
            }
        }

        this.executor = new ThreadPoolExecutor(
                poolSize,
                poolSize,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(poolSize * 1000),
                r -> {
                    Thread t = new Thread(r, "replication-worker");
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.CallerRunsPolicy());
        this.timeoutScheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "replication-timeout");
            t.setDaemon(true);
            return t;
        });
        this.nodeClients = new ConcurrentHashMap<>();
        this.readRepairHandler = new ReadRepairHandler(clusterManager, replicationFactor);
        this.internalAuthToken = readInternalToken();
    }

    /**
     * Write to replicas with the specified consistency level.
     *
     * @param key         the key to write
     * @param value       the value to write
     * @param timestamp   the authoritative timestamp for conflict resolution
     * @param expiresAt   the expiration timestamp (0 = no expiration)
     * @param consistency the consistency level required
     * @return CompletableFuture that completes when consistency is met
     */
    public CompletableFuture<Boolean> write(String key, byte[] value, long timestamp, long expiresAt,
            ConsistencyLevel consistency) {
        List<Node> replicas = clusterManager.getNodesForKey(key, replicationFactor);
        int required = consistency.getRequired(replicas.size());

        if (replicas.size() < required) {
            return CompletableFuture.failedFuture(
                    new ConsistencyException("Not enough replicas available", consistency, required, replicas.size()));
        }

        CompletableFuture<Boolean> result = new CompletableFuture<>();
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        int totalReplicas = replicas.size();

        for (Node replica : replicas) {
            executor.submit(() -> {
                try {
                    writeToNode(replica, key, value, timestamp, expiresAt);
                    int successes = successCount.incrementAndGet();
                    if (successes >= required && !result.isDone()) {
                        result.complete(true);
                    }
                } catch (IOException | RuntimeException e) {
                    logger.warn("Write to {} failed: {}", replica.getId(), e.getMessage());
                    int failures = failureCount.incrementAndGet();
                    if (failures > (totalReplicas - required) && !result.isDone()) {
                        result.completeExceptionally(
                                new ConsistencyException("Cannot meet consistency level",
                                        consistency, required, successCount.get()));
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
     * For QUORUM/ALL, uses ReadRepairHandler to compare timestamps and repair stale
     * replicas.
     *
     * @param key         the key to read
     * @param consistency the consistency level required
     * @return CompletableFuture with the value and metadata
     */
    public CompletableFuture<Optional<ReadResult>> read(String key, ConsistencyLevel consistency) {
        List<Node> replicas = clusterManager.getNodesForKey(key, replicationFactor);
        int required = consistency.getRequired(replicas.size());

        if (replicas.size() < required) {
            return CompletableFuture.failedFuture(
                    new ConsistencyException("Not enough replicas available", consistency, required, replicas.size()));
        }

        // ONE: just read from first replica (no repair)
        if (consistency == ConsistencyLevel.ONE) {
            return readFromNode(replicas.get(0), key);
        }

        // QUORUM/ALL: use read repair to get most recent value and fix stale replicas
        return readRepairHandler.readWithRepair(key, required, timeoutMs)
                .thenApply(result -> {
                    if (result.isRepairNeeded()) {
                        logger.info("Read repair performed for key {} on {} stale nodes",
                                key, result.getRepairedNodes().size());
                    }
                    if (result.getTimestamp() == 0) {
                        return Optional.empty();
                    }
                    return Optional.of(new ReadResult(result.getValue(),
                            result.getTimestamp(),
                            result.getExpiresAt()));
                });
    }

    /**
     * Delete from replicas with the specified consistency level.
     * Writes tombstones with the provided timestamp to prevent resurrection.
     *
     * @param key         the key to delete
     * @param timestamp   the authoritative timestamp for the tombstone
     * @param expiresAt   the expiration timestamp (0 = no expiration)
     * @param consistency the consistency level required
     * @return CompletableFuture that completes when consistency is met
     */
    public CompletableFuture<Boolean> delete(String key, long timestamp, long expiresAt, ConsistencyLevel consistency) {
        List<Node> replicas = clusterManager.getNodesForKey(key, replicationFactor);
        int required = consistency.getRequired(replicas.size());

        if (replicas.size() < required) {
            return CompletableFuture.failedFuture(
                    new ConsistencyException("Not enough replicas available", consistency, required, replicas.size()));
        }

        CompletableFuture<Boolean> result = new CompletableFuture<>();
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        int totalReplicas = replicas.size();

        for (Node replica : replicas) {
            executor.submit(() -> {
                try {
                    deleteFromNode(replica, key, timestamp, expiresAt);
                    int successes = successCount.incrementAndGet();
                    if (successes >= required && !result.isDone()) {
                        result.complete(true);
                    }
                } catch (IOException | RuntimeException e) {
                    logger.warn("Delete from {} failed: {}", replica.getId(), e.getMessage());
                    int failures = failureCount.incrementAndGet();
                    if (failures > (totalReplicas - required) && !result.isDone()) {
                        result.completeExceptionally(
                                new ConsistencyException("Cannot meet consistency level",
                                        consistency, required, successCount.get()));
                    }
                }
            });
        }

        scheduleTimeout(result, consistency, required);

        return result;
    }

    private void writeToNode(Node node, String key, byte[] value, long timestamp, long expiresAt) throws IOException {
        TitanKVClient client = getClient(node);
        // Use internal put to prevent replication cascade
        // Pass timestamp and expiresAt to maintain newest-wins semantics across
        // replicas
        client.putInternal(key, value, timestamp, expiresAt);
    }

    private CompletableFuture<Optional<ReadResult>> readFromNode(Node node, String key) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                TitanKVClient client = getClient(node);
                // Use internal get to prevent replication recursion and get timestamps
                Optional<TitanKVClient.ValueWithMetadata> result = client.getInternalWithMetadata(key);
                if (result.isPresent()) {
                    TitanKVClient.ValueWithMetadata metadata = result.get();
                    return Optional.of(new ReadResult(
                            metadata.getValue(),
                            metadata.getTimestamp(),
                            metadata.getExpiresAt()));
                }
                return Optional.empty();
            } catch (IOException e) {
                throw new CompletionException(e);
            }
        }, executor);
    }

    private void deleteFromNode(Node node, String key, long timestamp, long expiresAt) throws IOException {
        TitanKVClient client = getClient(node);
        // Use internal delete to prevent replication cascade
        // Pass timestamp and expiresAt to write tombstone with proper versioning
        client.deleteInternal(key, timestamp, expiresAt);
    }

    private TitanKVClient getClient(Node node) {
        return nodeClients.computeIfAbsent(node.getAddress(), addr -> {
            ClientConfig config = ClientConfig.builder()
                    .connectTimeoutMs((int) timeoutMs)
                    .readTimeoutMs((int) timeoutMs)
                    .retryOnFailure(false)
                    .authToken(internalAuthToken)
                    .build();
            return new TitanKVClient(config, addr);
        });
    }

    private static String readInternalToken() {
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
        return (value != null && !value.isEmpty()) ? value : null;
    }

    private <T> void scheduleTimeout(CompletableFuture<T> future, ConsistencyLevel level, int required) {
        timeoutScheduler.schedule(() -> {
            if (!future.isDone()) {
                future.completeExceptionally(
                        new ConsistencyException("Timeout waiting for consistency",
                                level, required, 0));
            }
        }, timeoutMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Get the replication factor.
     */
    public int getReplicationFactor() {
        return replicationFactor;
    }

    /**
     * Shutdown the replication manager and read repair handler.
     */
    public void shutdown() {
        executor.shutdown();
        timeoutScheduler.shutdown();
        readRepairHandler.shutdown();

        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
            if (!timeoutScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                timeoutScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            timeoutScheduler.shutdownNow();
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
