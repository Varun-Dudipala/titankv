package com.titankv.consistency;

import com.titankv.cluster.ClusterManager;
import com.titankv.cluster.Node;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class HintedHandoffTest {

    @TempDir
    Path hintsDir;

    private ClusterManager clusterManager;
    private Node replica;
    private final FakeReplicas replicas = new FakeReplicas();
    private final List<HintedHandoff> handoffs = new ArrayList<>();

    @BeforeEach
    void setUp() {
        clusterManager = new ClusterManager(new Node("local", "localhost", 18001));
        replica = new Node("replica", "localhost", 18002);
        clusterManager.addNode(replica);
        replica.setStatus(Node.Status.DEAD);
    }

    @AfterEach
    void tearDown() {
        handoffs.forEach(HintedHandoff::shutdown);
    }

    private HintedHandoff handoff(Path dir) {
        HintedHandoff handoff = new HintedHandoff(clusterManager, replicas, dir);
        handoffs.add(handoff);
        return handoff;
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private void recover() {
        replica.setStatus(Node.Status.SUSPECT);
        clusterManager.updateHeartbeat(replica.getId()); // fires NODE_RECOVERED
    }

    private void awaitDelivered(HintedHandoff handoff) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (handoff.pendingHints(replica.getId()) > 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertThat(handoff.pendingHints(replica.getId())).isZero();
    }

    @Test
    void hintsAreDeliveredWhenTheReplicaRecovers() throws Exception {
        HintedHandoff handoff = handoff(null);
        handoff.store(replica, "a", bytes("1"), 100, 0);
        handoff.store(replica, "b", null, 200, 0);
        assertThat(handoff.pendingHints(replica.getId())).isEqualTo(2);
        assertThat(replicas.writes).isEmpty();

        recover();
        awaitDelivered(handoff);
        assertThat(replicas.writes.get("a").timestamp).isEqualTo(100);
        assertThat(replicas.writes.get("b").value).isNull(); // tombstone delivered as a delete
    }

    @Test
    void hintsForTheSameKeyKeepOnlyTheNewestVersion() throws Exception {
        HintedHandoff handoff = handoff(null);
        handoff.store(replica, "k", bytes("new"), 300, 0);
        handoff.store(replica, "k", bytes("old"), 200, 0);
        assertThat(handoff.pendingHints(replica.getId())).isEqualTo(1);

        recover();
        awaitDelivered(handoff);
        assertThat(new String(replicas.writes.get("k").value, StandardCharsets.UTF_8)).isEqualTo("new");
    }

    @Test
    void failedDeliveryKeepsHintsForTheNextAttempt() throws Exception {
        HintedHandoff handoff = handoff(null);
        handoff.store(replica, "k", bytes("v"), 100, 0);
        replicas.failing.set(true);
        recover();
        Thread.sleep(300);
        assertThat(handoff.pendingHints(replica.getId())).isEqualTo(1);

        replicas.failing.set(false);
        awaitDelivered(handoff); // periodic retry
        assertThat(replicas.writes).containsKey("k");
    }

    @Test
    void persistedHintsSurviveACoordinatorRestart() throws Exception {
        HintedHandoff before = handoff(hintsDir);
        before.store(replica, "k1", bytes("v1"), 100, 0);
        before.store(replica, "k2", bytes("v2"), 200, 5_000);
        before.shutdown();

        HintedHandoff after = handoff(hintsDir);
        assertThat(after.pendingHints(replica.getId())).isEqualTo(2);
        recover();
        awaitDelivered(after);
        assertThat(replicas.writes.get("k2").expiresAt).isEqualTo(5_000);
    }

    @Test
    void hintFileIsCompactedWhenOverwritesMakeItLarge() throws Exception {
        HintedHandoff handoff = handoff(hintsDir);
        for (int i = 0; i < 50_000; i++) {
            handoff.store(replica, "hot-" + (i % 10), bytes("v" + i), 1_000 + i, 0);
        }
        long size = java.nio.file.Files.size(hintsDir.resolve("replica.hints"));
        assertThat(handoff.pendingHints(replica.getId())).isEqualTo(10);
        assertThat(size).as("hint file bytes").isLessThan(1_000_000);
        handoff.shutdown();

        HintedHandoff reloaded = handoff(hintsDir);
        assertThat(reloaded.pendingHints(replica.getId())).isEqualTo(10);
        recover();
        awaitDelivered(reloaded);
        assertThat(replicas.writes.get("hot-9").timestamp).isEqualTo(1_000 + 49_999);
    }

    @Test
    void hintsForARemovedNodeAreDropped() throws Exception {
        HintedHandoff handoff = handoff(hintsDir);
        handoff.store(replica, "k", bytes("v"), 100, 0);
        assertThat(hintsDir.resolve("replica.hints")).exists();

        clusterManager.removeDeadNode(replica.getId()); // fires NODE_LEFT

        long deadline = System.currentTimeMillis() + 5_000;
        while (handoff.pendingHints(replica.getId()) > 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertThat(handoff.totalPendingHints()).isZero();
        assertThat(hintsDir.resolve("replica.hints")).doesNotExist();
    }

    /**
     * Records writes, or fails them all while {@code failing} is set.
     */
    private static final class FakeReplicas implements ReplicaIO {
        record Write(byte[] value, long timestamp, long expiresAt) {
        }

        final Map<String, Write> writes = new ConcurrentHashMap<>();
        final AtomicBoolean failing = new AtomicBoolean();

        @Override
        public Optional<ReplicationManager.ReadResult> read(Node replica, String key) {
            return Optional.empty();
        }

        @Override
        public void write(Node replica, String key, byte[] value, long timestamp, long expiresAt) throws IOException {
            if (failing.get()) {
                throw new IOException("replica unreachable");
            }
            writes.put(key, new Write(value, timestamp, expiresAt));
        }
    }
}
