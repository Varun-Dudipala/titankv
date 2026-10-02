package com.titankv.consistency;

import com.titankv.cluster.ClusterManager;
import com.titankv.cluster.Node;
import com.titankv.core.KeyValuePair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;

/**
 * Read coordination against three in-memory replicas: newest version wins, stale contacted
 * replicas are repaired, failed replicas are replaced by spares, and too few answers fail the read.
 */
class ReadRepairHandlerTest {

    private final FakeReplicas replicas = new FakeReplicas();
    private ClusterManager clusterManager;
    private Node local;
    private Node peer1;
    private Node peer2;
    private ExecutorService executor;
    private ReadRepairHandler handler;

    @BeforeEach
    void setUp() {
        local = new Node("local", "localhost", 19301);
        peer1 = new Node("peer1", "localhost", 19302);
        peer2 = new Node("peer2", "localhost", 19303);
        clusterManager = new ClusterManager(local);
        clusterManager.addNode(peer1);
        clusterManager.addNode(peer2);
        executor = Executors.newFixedThreadPool(4);
        handler = new ReadRepairHandler(clusterManager, 3, replicas, executor);
    }

    @AfterEach
    void tearDown() {
        handler.shutdown();
        executor.shutdownNow();
    }

    private ReadRepairHandler.RepairResult read(int required) throws Exception {
        return handler.readWithRepair("k", required, 2_000).get(3, TimeUnit.SECONDS);
    }

    @Test
    void returnsTheNewestVersionAndRepairsTheStaleReplicaItContacted() throws Exception {
        replicas.put(local, "k", "old", 1);
        replicas.put(peer1, "k", "new", 2);
        replicas.put(peer2, "k", "new", 2);

        ReadRepairHandler.RepairResult result = read(2);

        assertThat(string(result.getValue())).isEqualTo("new");
        assertThat(result.getRepairedNodes()).containsExactly(local);
        awaitValue(local, "new");
        assertThat(handler.getReplicasRepaired()).isEqualTo(1);
    }

    @Test
    void equalVersionsResolveToTheSameWinnerAndRepairConverges() throws Exception {
        replicas.put(local, "k", "apple", 5);
        replicas.put(peer1, "k", "banana", 5);
        replicas.put(peer2, "k", "banana", 5);

        assertThat(string(read(3).getValue())).isEqualTo("banana");
        awaitValue(local, "banana");
    }

    @Test
    void aTombstoneIsTheNewestVersion() throws Exception {
        replicas.put(local, "k", "value", 1);
        replicas.store(peer1).put("k", new KeyValuePair(null, 2, 0));
        replicas.store(peer2).put("k", new KeyValuePair(null, 2, 0));

        ReadRepairHandler.RepairResult result = read(3);

        assertThat(result.getValue()).isNull();
        assertThat(result.getTimestamp()).isEqualTo(2);
    }

    @Test
    void aKeyNoReplicaHasReadsAsVersionZero() throws Exception {
        assertThat(read(2).getTimestamp()).isZero();
    }

    @Test
    void aFailedReplicaIsReplacedByTheSpare() throws Exception {
        replicas.put(local, "k", "v", 1);
        replicas.put(peer1, "k", "v", 1);
        replicas.put(peer2, "k", "v", 1);
        replicas.failing.add(peer1);

        assertThat(string(read(2).getValue())).isEqualTo("v");
    }

    @Test
    void tooFewAnswersFailTheRead() {
        replicas.failing.add(peer1);
        replicas.failing.add(peer2);

        assertThatThrownBy(() -> read(2)).hasCauseInstanceOf(ConsistencyException.class)
                .hasMessageContaining("Not enough replicas");
    }

    @Test
    void downReplicasAreNotAskedAndAllFailsAtOnce() {
        peer2.setStatus(Node.Status.DEAD);

        assertThatThrownBy(() -> read(3)).hasCauseInstanceOf(ConsistencyException.class);
        assertThat(replicas.reads.getOrDefault(peer2, 0)).isZero();
    }

    @Test
    void aSlowReplicaIsBackedUpBySpeculativelyAskingTheSpare() throws Exception {
        replicas.put(local, "k", "v", 1);
        replicas.put(peer1, "k", "v", 1);
        replicas.put(peer2, "k", "v", 1);
        replicas.slow.add(local); // asked first, hangs for 3 seconds

        long start = System.nanoTime();
        ReadRepairHandler.RepairResult result = read(1);
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        assertThat(string(result.getValue())).isEqualTo("v");
        assertThat(elapsedMs).isLessThan(1_000);
        assertThat(handler.getSpeculativeReads()).isEqualTo(1);
    }

    private void awaitValue(Node node, String expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 3_000;
        while (System.currentTimeMillis() < deadline) {
            KeyValuePair kv = replicas.store(node).get("k");
            if (kv != null && expected.equals(string(kv.getValueUnsafe()))) {
                return;
            }
            Thread.sleep(10);
        }
        fail(node.getId() + " was not repaired to " + expected);
    }

    private static String string(byte[] bytes) {
        return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Replicas as maps that apply writes with last-write-wins, like the real store.
     */
    private static final class FakeReplicas implements ReplicaIO {
        final Map<Node, Map<String, KeyValuePair>> stores = new ConcurrentHashMap<>();
        final Map<Node, Integer> reads = new ConcurrentHashMap<>();
        final Set<Node> failing = ConcurrentHashMap.newKeySet();
        final Set<Node> slow = ConcurrentHashMap.newKeySet();

        Map<String, KeyValuePair> store(Node node) {
            return stores.computeIfAbsent(node, n -> new ConcurrentHashMap<>());
        }

        void put(Node node, String key, String value, long timestamp) {
            store(node).put(key, new KeyValuePair(value.getBytes(StandardCharsets.UTF_8), timestamp, 0));
        }

        @Override
        public Optional<ReplicationManager.ReadResult> read(Node replica, String key) throws IOException {
            reads.merge(replica, 1, Integer::sum);
            if (failing.contains(replica)) {
                throw new IOException("replica unreachable");
            }
            if (slow.contains(replica)) {
                try {
                    Thread.sleep(3_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return Optional.ofNullable(store(replica).get(key))
                    .map(kv -> new ReplicationManager.ReadResult(kv.getValue(), kv.getTimestamp(), kv.getExpiresAt()));
        }

        @Override
        public void write(Node replica, String key, byte[] value, long timestamp, long expiresAt) {
            KeyValuePair entry = new KeyValuePair(value, timestamp, expiresAt);
            store(replica).merge(key, entry, (current, fresh) -> fresh.isNewerThan(current) ? fresh : current);
        }
    }
}
