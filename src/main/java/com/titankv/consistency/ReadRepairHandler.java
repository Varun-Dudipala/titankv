package com.titankv.consistency;

import com.titankv.TitanKVClient;
import com.titankv.client.ClientConfig;
import com.titankv.cluster.ClusterManager;
import com.titankv.cluster.Node;
import com.titankv.util.Env;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Reads a key from its replicas, returns the newest version once enough replicas have
 * answered, and writes that version back to any replica that returned an older one.
 */
public class ReadRepairHandler {

    private static final Logger logger = LoggerFactory.getLogger(ReadRepairHandler.class);

    private final ClusterManager clusterManager;
    private final int replicationFactor;
    private final ReplicaIO replicaIO;
    private final ExecutorService executor;
    private final boolean ownsExecutor;
    private final Map<String, TitanKVClient> ownedClients = new ConcurrentHashMap<>();

    /**
     * Create a standalone handler with its own threads and authenticated node connections.
     */
    public ReadRepairHandler(ClusterManager clusterManager, int replicationFactor) {
        this(clusterManager, replicationFactor, null, null);
    }

    /**
     * @param replicaIO how to reach replicas, or null to connect over TCP with the internal token
     * @param executor  pool for blocking replica calls, or null to create a private one
     */
    public ReadRepairHandler(ClusterManager clusterManager, int replicationFactor,
            ReplicaIO replicaIO, ExecutorService executor) {
        this.clusterManager = clusterManager;
        this.replicationFactor = replicationFactor;
        this.replicaIO = replicaIO != null ? replicaIO : new TcpReplicaIO();
        this.ownsExecutor = executor == null;
        this.executor = executor != null ? executor : Executors.newFixedThreadPool(4, r -> {
            Thread t = new Thread(r, "read-repair");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Read result: the newest version seen and which replicas were found stale.
     */
    public static class RepairResult {
        private final byte[] value;
        private final long timestamp;
        private final long expiresAt;
        private final List<Node> repairedNodes;
        private final boolean repairNeeded;

        public RepairResult(byte[] value, long timestamp, long expiresAt,
                List<Node> repairedNodes, boolean repairNeeded) {
            this.value = value;
            this.timestamp = timestamp;
            this.expiresAt = expiresAt;
            this.repairedNodes = repairedNodes;
            this.repairNeeded = repairNeeded;
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

        public List<Node> getRepairedNodes() {
            return repairedNodes;
        }

        public boolean isRepairNeeded() {
            return repairNeeded;
        }
    }

    /**
     * Query every replica. The returned future completes with the newest version among the first
     * {@code requiredResponses} answers; once all replicas have answered (or the timeout passes),
     * stale replicas are repaired in the background.
     *
     * @param requiredResponses answers needed before returning (1 for ONE, a majority for QUORUM)
     * @param timeoutMs         how long to wait for enough answers
     */
    public CompletableFuture<RepairResult> readWithRepair(String key, int requiredResponses, long timeoutMs) {
        List<Node> replicas = liveReplicas(key);
        if (replicas.size() < requiredResponses) {
            return CompletableFuture.failedFuture(
                    new ConsistencyException("Not enough replicas responded", requiredResponses, 0));
        }

        CompletableFuture<RepairResult> result = new CompletableFuture<>();
        List<NodeValue> responses = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger failures = new AtomicInteger();
        List<CompletableFuture<Void>> reads = new ArrayList<>();

        for (Node replica : replicas) {
            reads.add(CompletableFuture.runAsync(() -> {
                try {
                    NodeValue response = readReplica(replica, key);
                    List<NodeValue> snapshot;
                    synchronized (responses) {
                        responses.add(response);
                        snapshot = new ArrayList<>(responses);
                    }
                    if (snapshot.size() >= requiredResponses) {
                        result.complete(summarize(snapshot));
                    }
                } catch (IOException | RuntimeException e) {
                    logger.debug("Read from {} failed: {}", replica.getId(), e.getMessage());
                    if (replicas.size() - failures.incrementAndGet() < requiredResponses) {
                        result.completeExceptionally(new ConsistencyException(
                                "Not enough replicas responded", requiredResponses, responses.size()));
                    }
                }
            }, executor));
        }

        CompletableFuture.allOf(reads.toArray(new CompletableFuture<?>[0]))
                .orTimeout(timeoutMs, TimeUnit.MILLISECONDS)
                .whenComplete((ignored, error) -> {
                    List<NodeValue> all;
                    synchronized (responses) {
                        all = new ArrayList<>(responses);
                    }
                    repairStale(key, all);
                });

        return result.orTimeout(timeoutMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Write the given version to every replica of the key.
     *
     * @return number of replicas that accepted the write
     */
    public CompletableFuture<Integer> forceRepair(String key, byte[] value, long timestamp, long expiresAt) {
        List<Node> replicas = liveReplicas(key);
        List<CompletableFuture<Boolean>> writes = new ArrayList<>();
        for (Node node : replicas) {
            writes.add(CompletableFuture.supplyAsync(() -> {
                try {
                    replicaIO.write(node, key, value, timestamp, expiresAt);
                    return true;
                } catch (IOException | RuntimeException e) {
                    logger.warn("Failed to repair {} on {}: {}", key, node.getId(), e.getMessage());
                    return false;
                }
            }, executor));
        }
        return CompletableFuture.allOf(writes.toArray(new CompletableFuture<?>[0]))
                .thenApply(v -> (int) writes.stream().filter(CompletableFuture::join).count());
    }

    /**
     * The key's replicas that are currently up. Down replicas are not substituted by other nodes.
     */
    private List<Node> liveReplicas(String key) {
        List<Node> live = new ArrayList<>();
        for (Node replica : clusterManager.getReplicasForKey(key, replicationFactor)) {
            if (replica.isAvailable()) {
                live.add(replica);
            }
        }
        return live;
    }

    private NodeValue readReplica(Node node, String key) throws IOException {
        Optional<ReplicationManager.ReadResult> result = replicaIO.read(node, key);
        return result
                .map(r -> new NodeValue(node, r.getValue(), r.getTimestamp(), r.getExpiresAt()))
                .orElseGet(() -> new NodeValue(node, null, 0, 0));
    }

    private RepairResult summarize(List<NodeValue> responses) {
        NodeValue newest = findNewest(responses);
        if (newest == null) {
            return new RepairResult(null, 0, 0, Collections.emptyList(), false);
        }
        List<Node> stale = findStaleNodes(responses, newest);
        return new RepairResult(newest.value, newest.timestamp, newest.expiresAt, stale, !stale.isEmpty());
    }

    private void repairStale(String key, List<NodeValue> responses) {
        NodeValue newest = findNewest(responses);
        if (newest == null) {
            return;
        }
        List<Node> stale = findStaleNodes(responses, newest);
        if (stale.isEmpty()) {
            return;
        }
        logger.debug("Read repair for key {} on {} stale replicas", key, stale.size());
        for (Node node : stale) {
            try {
                executor.execute(() -> {
                    try {
                        replicaIO.write(node, key, newest.value, newest.timestamp, newest.expiresAt);
                    } catch (IOException | RuntimeException e) {
                        logger.warn("Failed to repair {} on {}: {}", key, node.getId(), e.getMessage());
                    }
                });
            } catch (RejectedExecutionException e) {
                return; // shutting down
            }
        }
    }

    /**
     * Last write wins. Ties on timestamp are broken by comparing values so every
     * coordinator picks the same winner.
     */
    private static NodeValue findNewest(List<NodeValue> results) {
        NodeValue newest = null;
        for (NodeValue nv : results) {
            if (nv.timestamp <= 0) {
                continue;
            }
            if (newest == null || nv.timestamp > newest.timestamp
                    || (nv.timestamp == newest.timestamp && compareValues(nv.value, newest.value) > 0)) {
                newest = nv;
            }
        }
        return newest;
    }

    private static int compareValues(byte[] a, byte[] b) {
        if (a == b) {
            return 0;
        }
        if (a == null) {
            return -1;
        }
        if (b == null) {
            return 1;
        }
        return Arrays.compare(a, b);
    }

    private static List<Node> findStaleNodes(List<NodeValue> results, NodeValue newest) {
        List<Node> stale = new ArrayList<>();
        for (NodeValue nv : results) {
            if (!nv.node.equals(newest.node)
                    && (nv.timestamp < newest.timestamp || !Arrays.equals(nv.value, newest.value))) {
                stale.add(nv.node);
            }
        }
        return stale;
    }

    public void shutdown() {
        if (ownsExecutor) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        for (TitanKVClient client : ownedClients.values()) {
            client.close();
        }
        ownedClients.clear();
    }

    /**
     * Reaches replicas over TCP using internal commands, for handlers not given a ReplicaIO.
     */
    private class TcpReplicaIO implements ReplicaIO {
        @Override
        public Optional<ReplicationManager.ReadResult> read(Node replica, String key) throws IOException {
            return client(replica).getInternalWithMetadata(key)
                    .map(m -> new ReplicationManager.ReadResult(m.getValue(), m.getTimestamp(), m.getExpiresAt()));
        }

        @Override
        public void write(Node replica, String key, byte[] value, long timestamp, long expiresAt)
                throws IOException {
            if (value == null) {
                client(replica).deleteInternal(key, timestamp, expiresAt);
            } else {
                client(replica).putInternal(key, value, timestamp, expiresAt);
            }
        }

        private TitanKVClient client(Node node) {
            return ownedClients.computeIfAbsent(node.getAddress(), addr -> new TitanKVClient(
                    ClientConfig.builder()
                            .connectTimeoutMs(5000)
                            .readTimeoutMs(5000)
                            .retryOnFailure(false)
                            .circuitBreaker(false)
                            .authToken(Env.internalToken())
                            .build(),
                    addr));
        }
    }

    private static final class NodeValue {
        final Node node;
        final byte[] value;
        final long timestamp;
        final long expiresAt;

        NodeValue(Node node, byte[] value, long timestamp, long expiresAt) {
            this.node = node;
            this.value = value;
            this.timestamp = timestamp;
            this.expiresAt = expiresAt;
        }
    }
}
