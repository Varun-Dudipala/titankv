package com.titankv.consistency;

import com.titankv.TitanKVClient;
import com.titankv.client.ClientConfig;
import com.titankv.cluster.ClusterManager;
import com.titankv.cluster.Node;
import com.titankv.core.KVStore;
import com.titankv.core.KeyValuePair;
import com.titankv.util.Env;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Coordinates replicated reads, writes and deletes with tunable consistency.
 * Any node can coordinate any key: the request is sent to the key's replicas and
 * completes once the consistency level is met.
 */
public final class ReplicationManager implements ReplicaIO {

    private static final Logger logger = LoggerFactory.getLogger(ReplicationManager.class);

    private static final int DEFAULT_REPLICATION_FACTOR = 3;
    private static final long DEFAULT_TIMEOUT_MS = 5000;

    private final ClusterManager clusterManager;
    private final int replicationFactor;
    private final long timeoutMs;
    private final int poolSize;
    private final ExecutorService executor;
    private final Map<String, TitanKVClient> nodeClients;
    private final ReadRepairHandler readRepairHandler;
    private final String internalAuthToken;
    private final KVStore localStore;

    /**
     * Read result with value metadata. A null value with a non-zero timestamp is a tombstone.
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

    public ReplicationManager(ClusterManager clusterManager) {
        this(clusterManager, DEFAULT_REPLICATION_FACTOR, DEFAULT_TIMEOUT_MS, null);
    }

    public ReplicationManager(ClusterManager clusterManager, KVStore localStore) {
        this(clusterManager, DEFAULT_REPLICATION_FACTOR, DEFAULT_TIMEOUT_MS, localStore);
    }

    public ReplicationManager(ClusterManager clusterManager, int replicationFactor, long timeoutMs) {
        this(clusterManager, replicationFactor, timeoutMs, null);
    }

    /**
     * @param localStore this node's store; the local replica is read and written directly
     *                   instead of over the network. Null sends every replica request over TCP.
     */
    public ReplicationManager(ClusterManager clusterManager, int replicationFactor, long timeoutMs,
            KVStore localStore) {
        this.clusterManager = clusterManager;
        this.replicationFactor = replicationFactor;
        this.timeoutMs = timeoutMs;
        this.localStore = localStore;

        int threads = Math.max(16, Runtime.getRuntime().availableProcessors() * 4);
        String configured = Env.get("TITANKV_REPLICATION_THREADS", "titankv.replication.threads");
        if (configured != null) {
            try {
                threads = Integer.parseInt(configured.trim());
            } catch (NumberFormatException e) {
                logger.warn("Invalid TITANKV_REPLICATION_THREADS, using default {}", threads);
            }
        }
        this.poolSize = threads;

        // Replica calls use blocking sockets, so this pool is sized for I/O wait, not CPU.
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
        this.nodeClients = new ConcurrentHashMap<>();
        this.internalAuthToken = Env.internalToken();
        this.readRepairHandler = new ReadRepairHandler(clusterManager, replicationFactor, this, executor);
    }

    /**
     * Number of replicas a key should have: the replication factor, capped by the number of
     * cluster members. Members that are down still count, so losing replicas makes QUORUM and
     * ALL fail rather than quietly requiring fewer acknowledgements.
     */
    int effectiveReplicationFactor() {
        return Math.max(1, Math.min(replicationFactor, clusterManager.getNodeCount()));
    }

    /**
     * Write to replicas with the specified consistency level.
     *
     * @return future that completes when the consistency level is met
     */
    public CompletableFuture<Boolean> write(String key, byte[] value, long timestamp, long expiresAt,
            ConsistencyLevel consistency) {
        return replicate(key, consistency, "Write",
                replica -> write(replica, key, value, timestamp, expiresAt));
    }

    /**
     * Delete from replicas by writing a tombstone with the given timestamp, which
     * prevents stale replicas from resurrecting the value.
     */
    public CompletableFuture<Boolean> delete(String key, long timestamp, long expiresAt,
            ConsistencyLevel consistency) {
        return replicate(key, consistency, "Delete",
                replica -> write(replica, key, null, timestamp, expiresAt));
    }

    private interface ReplicaOperation {
        void apply(Node replica) throws IOException;
    }

    private CompletableFuture<Boolean> replicate(String key, ConsistencyLevel consistency, String name,
            ReplicaOperation operation) {
        int required = consistency.getRequired(effectiveReplicationFactor());
        List<Node> replicas = clusterManager.getNodesForKey(key, replicationFactor);
        if (replicas.size() < required) {
            return CompletableFuture.failedFuture(new ConsistencyException(
                    "Not enough replicas available", consistency, required, replicas.size()));
        }

        CompletableFuture<Boolean> result = new CompletableFuture<>();
        AtomicInteger successes = new AtomicInteger();
        AtomicInteger failures = new AtomicInteger();
        int allowedFailures = replicas.size() - required;

        for (Node replica : replicas) {
            executor.execute(() -> {
                try {
                    operation.apply(replica);
                    if (successes.incrementAndGet() >= required) {
                        result.complete(true);
                    }
                } catch (IOException | RuntimeException e) {
                    logger.warn("{} to {} failed: {}", name, replica.getId(), e.getMessage());
                    if (failures.incrementAndGet() > allowedFailures) {
                        result.completeExceptionally(new ConsistencyException(
                                "Cannot meet consistency level", consistency, required, successes.get()));
                    }
                }
            });
        }
        return result.orTimeout(timeoutMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Read from replicas with the specified consistency level. Returns the newest version among
     * the first responses that satisfy the level, and repairs stale replicas in the background.
     *
     * @return the newest version found, or empty if no replica has the key. A tombstone is
     *         returned as a result with a null value.
     */
    public CompletableFuture<Optional<ReadResult>> read(String key, ConsistencyLevel consistency) {
        int required = consistency.getRequired(effectiveReplicationFactor());
        return readRepairHandler.readWithRepair(key, required, timeoutMs)
                .thenApply(result -> {
                    if (result.getTimestamp() == 0) {
                        return Optional.empty();
                    }
                    return Optional.of(new ReadResult(result.getValue(), result.getTimestamp(),
                            result.getExpiresAt()));
                });
    }

    @Override
    public Optional<ReadResult> read(Node replica, String key) throws IOException {
        if (isLocal(replica)) {
            Optional<KeyValuePair> entry = localStore.getRaw(key);
            return entry.map(kv -> new ReadResult(kv.getValue(), kv.getTimestamp(), kv.getExpiresAt()));
        }
        return getClient(replica).getInternalWithMetadata(key)
                .map(m -> new ReadResult(m.getValue(), m.getTimestamp(), m.getExpiresAt()));
    }

    @Override
    public void write(Node replica, String key, byte[] value, long timestamp, long expiresAt) throws IOException {
        if (isLocal(replica)) {
            localStore.putIfNewer(key, value, timestamp, expiresAt);
            return;
        }
        TitanKVClient client = getClient(replica);
        if (value == null) {
            client.deleteInternal(key, timestamp, expiresAt);
        } else {
            client.putInternal(key, value, timestamp, expiresAt);
        }
    }

    private boolean isLocal(Node replica) {
        return localStore != null && replica.equals(clusterManager.getLocalNode());
    }

    private TitanKVClient getClient(Node node) {
        return nodeClients.computeIfAbsent(node.getAddress(), addr -> {
            ClientConfig config = ClientConfig.builder()
                    .connectTimeoutMs((int) timeoutMs)
                    .readTimeoutMs((int) timeoutMs)
                    .maxConnectionsPerHost(poolSize)
                    .retryOnFailure(false)
                    .authToken(internalAuthToken)
                    .build();
            return new TitanKVClient(config, addr);
        });
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public void shutdown() {
        readRepairHandler.shutdown();
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

        logger.info("Replication manager shutdown complete");
    }
}
