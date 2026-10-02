package com.titankv.consistency;

import com.titankv.cluster.ClusterManager;
import com.titankv.cluster.Node;
import com.titankv.core.KeyValuePair;
import com.titankv.util.Env;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

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
    private final java.util.concurrent.atomic.AtomicLong replicasRepaired = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong speculativeReads = new java.util.concurrent.atomic.AtomicLong();

    /** Reads with a spare replica that may still need a speculative request. */
    private final Queue<ReadSession> speculationCandidates = new ConcurrentLinkedQueue<>();
    private final long speculativeRetryNanos = TimeUnit.MILLISECONDS.toNanos(speculativeRetryMs());
    private final ScheduledExecutorService speculationSweeper;

    /**
     * @param replicaIO how to read and write individual replicas
     * @param executor  pool for blocking replica calls, or null to create a private one
     */
    public ReadRepairHandler(ClusterManager clusterManager, int replicationFactor,
            ReplicaIO replicaIO, ExecutorService executor) {
        this.clusterManager = clusterManager;
        this.replicationFactor = replicationFactor;
        this.replicaIO = Objects.requireNonNull(replicaIO);
        this.ownsExecutor = executor == null;
        this.executor = executor != null ? executor : Executors.newFixedThreadPool(4, r -> {
            Thread t = new Thread(r, "read-repair");
            t.setDaemon(true);
            return t;
        });
        if (speculativeRetryNanos > 0) {
            // One periodic sweep instead of a timer per read, so fast reads pay nothing for it
            this.speculationSweeper = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "read-speculation");
                t.setDaemon(true);
                return t;
            });
            long period = Math.max(1, speculativeRetryNanos / 5);
            speculationSweeper.scheduleAtFixedRate(this::sweepSpeculation, period, period, TimeUnit.NANOSECONDS);
        } else {
            this.speculationSweeper = null;
        }
    }

    /**
     * How long a read waits on a slow replica before also asking a spare one (Cassandra's
     * speculative retry). 0 disables it.
     */
    private static long speculativeRetryMs() {
        String configured = Env.get("TITANKV_SPECULATIVE_RETRY_MS", "titankv.speculative.retry.ms");
        if (configured == null || configured.isBlank()) {
            return 50;
        }
        try {
            return Math.max(0, Long.parseLong(configured.trim()));
        } catch (NumberFormatException e) {
            logger.warn("Invalid speculative retry delay '{}', using 50 ms", configured);
            return 50;
        }
    }

    private void sweepSpeculation() {
        long now = System.nanoTime();
        Iterator<ReadSession> it = speculationCandidates.iterator();
        while (it.hasNext()) {
            ReadSession session = it.next();
            if (session.result.isDone()) {
                it.remove();
            } else if (now - session.startedAt >= speculativeRetryNanos) {
                it.remove();
                try {
                    session.speculate();
                } catch (RuntimeException e) {
                    logger.debug("Speculative read failed to start: {}", e.getMessage());
                }
            }
        }
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
     * Read a key from as many replicas as the consistency level needs, local replica first, and
     * return the newest version among their answers. If a replica fails, the next live replica is
     * asked in its place, and if one is slow (TITANKV_SPECULATIVE_RETRY_MS, default 50 ms) a spare
     * replica is asked as well and whichever answers first counts. Once every contacted replica has answered, any of them holding an older
     * version is repaired in the background. Replicas that were not contacted are brought up to
     * date by hinted handoff and anti-entropy instead.
     *
     * @param requiredResponses answers needed (1 for ONE, a majority for QUORUM, all for ALL)
     * @param timeoutMs         how long to wait for enough answers
     */
    public CompletableFuture<RepairResult> readWithRepair(String key, int requiredResponses, long timeoutMs) {
        List<Node> replicas = liveReplicas(key);
        if (replicas.size() < requiredResponses) {
            return CompletableFuture.failedFuture(
                    new ConsistencyException("Not enough replicas responded", requiredResponses, 0));
        }
        // Local replica first: it answers in-process without a network round trip
        Node local = clusterManager.getLocalNode();
        replicas.sort(Comparator.comparing(node -> !node.equals(local)));

        ReadSession session = new ReadSession(key, replicas, requiredResponses);
        session.start();
        if (speculationSweeper != null && replicas.size() > requiredResponses && !session.result.isDone()) {
            speculationCandidates.add(session);
        }
        return session.result.orTimeout(timeoutMs, TimeUnit.MILLISECONDS);
    }

    /**
     * State of one read: which replicas have been asked, and their answers.
     */
    private final class ReadSession {
        private final String key;
        private final List<Node> replicas;
        private final int required;
        private final CompletableFuture<RepairResult> result = new CompletableFuture<>();
        private final List<NodeValue> responses = new ArrayList<>();
        private final long startedAt = System.nanoTime();
        private int nextReplica;
        private int outstanding;

        ReadSession(String key, List<Node> replicas, int required) {
            this.key = key;
            this.replicas = replicas;
            this.required = required;
        }

        void start() {
            List<Node> first;
            synchronized (this) {
                first = new ArrayList<>(replicas.subList(0, required));
                nextReplica = required;
                outstanding = required;
            }
            // Send remote reads first, then do any in-process read on this thread meanwhile
            Node inProcess = null;
            for (Node replica : first) {
                if (inProcess == null && replicaIO.isInProcess(replica)) {
                    inProcess = replica;
                } else {
                    submit(replica);
                }
            }
            if (inProcess != null) {
                read(inProcess);
            }
        }

        /**
         * Ask the next spare replica too, because a contacted one is slow to answer.
         */
        void speculate() {
            Node spare;
            synchronized (this) {
                if (result.isDone() || nextReplica >= replicas.size()) {
                    return;
                }
                spare = replicas.get(nextReplica++);
                outstanding++;
            }
            speculativeReads.incrementAndGet();
            submit(spare);
        }

        private void submit(Node replica) {
            try {
                executor.execute(() -> read(replica));
            } catch (RejectedExecutionException e) {
                read(replica); // shutting down: answer on this thread
            }
        }

        private void read(Node replica) {
            NodeValue response = null;
            try {
                response = readReplica(replica, key);
            } catch (IOException | RuntimeException e) {
                logger.debug("Read from {} failed: {}", replica.getId(), e.getMessage());
            }

            Node spare = null;
            RepairResult complete = null;
            boolean fail = false;
            List<NodeValue> finished = null;
            synchronized (this) {
                outstanding--;
                if (response != null) {
                    responses.add(response);
                    if (responses.size() == required) {
                        complete = summarize(new ArrayList<>(responses));
                    }
                } else if (nextReplica < replicas.size()) {
                    spare = replicas.get(nextReplica++);
                    outstanding++;
                } else if (responses.size() + outstanding < required) {
                    fail = true;
                }
                if (outstanding == 0) {
                    finished = new ArrayList<>(responses);
                }
            }

            if (complete != null) {
                result.complete(complete);
            }
            if (fail) {
                result.completeExceptionally(new ConsistencyException(
                        "Not enough replicas responded", required, responses.size()));
            }
            if (spare != null) {
                submit(spare);
            }
            if (finished != null) {
                repairStale(key, finished);
            }
        }
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
        replicasRepaired.addAndGet(stale.size());
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
     * The winning version under last-write-wins, ignoring replicas that have no entry.
     */
    private static NodeValue findNewest(List<NodeValue> results) {
        NodeValue newest = null;
        for (NodeValue nv : results) {
            if (nv.timestamp <= 0) {
                continue;
            }
            if (newest == null
                    || KeyValuePair.compareVersions(nv.timestamp, nv.value, newest.timestamp, newest.value) > 0) {
                newest = nv;
            }
        }
        return newest;
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

    /**
     * @return stale replica copies this node has sent repairs for since startup
     */
    public long getReplicasRepaired() {
        return replicasRepaired.get();
    }

    /**
     * @return reads that also asked a spare replica because a contacted one was slow
     */
    public long getSpeculativeReads() {
        return speculativeReads.get();
    }

    public void shutdown() {
        if (speculationSweeper != null) {
            speculationSweeper.shutdownNow();
        }
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
