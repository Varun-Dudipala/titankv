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

    /**
     * Delivery and removal update the hints in memory first and the file just after, on the
     * handoff thread, so tests that look at the file wait for it.
     */
    private void awaitHintFile(java.util.function.Predicate<Path> condition, String description)
            throws InterruptedException {
        Path file = hintsDir.resolve("replica.hints");
        long deadline = System.currentTimeMillis() + 5_000;
        while (!condition.test(file) && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertThat(condition.test(file)).as(description).isTrue();
    }

    private static boolean holdsNoHints(Path file) {
        try {
            // Only the header is left: a length and the node id
            return java.nio.file.Files.size(file) == 4 + "replica".length();
        } catch (IOException e) {
            return false;
        }
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

    /**
     * Hints loaded from disk after a restart must leave the file once delivered, or every later
     * restart loads and delivers them again.
     */
    @Test
    void hintsDeliveredAfterARestartAreNotLoadedAgain() throws Exception {
        HintedHandoff before = handoff(hintsDir);
        before.store(replica, "k1", bytes("v1"), 100, 0);
        before.store(replica, "k2", null, 200, 0);
        before.shutdown();

        HintedHandoff restarted = handoff(hintsDir);
        assertThat(restarted.pendingHints(replica.getId())).isEqualTo(2);
        recover();
        awaitDelivered(restarted);
        awaitHintFile(HintedHandoffTest::holdsNoHints, "delivered hints removed from the file");
        restarted.shutdown();
        replicas.writes.clear();
        replica.setStatus(Node.Status.DEAD);

        HintedHandoff restartedAgain = handoff(hintsDir);
        assertThat(restartedAgain.pendingHints(replica.getId())).isZero();

        // The emptied file still takes new hints
        restartedAgain.store(replica, "k3", bytes("v3"), 300, 0);
        restartedAgain.shutdown();
        HintedHandoff last = handoff(hintsDir);
        assertThat(last.pendingHints(replica.getId())).isEqualTo(1);
        recover();
        awaitDelivered(last);
        assertThat(replicas.writes).containsOnlyKeys("k3");
    }

    @Test
    void hintsLoadedAfterARestartAreDroppedWithTheirFileWhenTheNodeIsRemoved() throws Exception {
        HintedHandoff before = handoff(hintsDir);
        before.store(replica, "k", bytes("v"), 100, 0);
        before.shutdown();

        HintedHandoff restarted = handoff(hintsDir);
        assertThat(restarted.pendingHints(replica.getId())).isEqualTo(1);
        clusterManager.removeDeadNode(replica.getId()); // fires NODE_LEFT

        awaitHintFile(file -> !java.nio.file.Files.exists(file), "the removed node's hint file deleted");
        assertThat(restarted.totalPendingHints()).isZero();
    }

    /**
     * A crash in the middle of an append leaves a torn record at the end of a hint file. The file
     * is appended to after the restart, so the torn record must go: hints written behind it would
     * be skipped by the next load.
     */
    @Test
    void hintsStoredAfterATornRecordSurviveTheNextRestart() throws Exception {
        HintedHandoff before = handoff(hintsDir);
        for (int i = 0; i < 6; i++) {
            before.store(replica, "k" + i, bytes("v" + i), 100 + i, 0);
        }
        before.shutdown();
        Path file = hintsDir.resolve("replica.hints");
        try (java.nio.channels.FileChannel channel = java.nio.channels.FileChannel.open(file,
                java.nio.file.StandardOpenOption.WRITE)) {
            channel.truncate(channel.size() - 3); // the sixth record is torn
        }

        HintedHandoff restarted = handoff(hintsDir);
        assertThat(restarted.pendingHints(replica.getId())).isEqualTo(5);
        restarted.store(replica, "after-1", bytes("a"), 200, 0);
        restarted.store(replica, "after-2", null, 201, 0);
        restarted.shutdown();

        HintedHandoff restartedAgain = handoff(hintsDir);
        assertThat(restartedAgain.pendingHints(replica.getId())).isEqualTo(7);
        recover();
        awaitDelivered(restartedAgain);
        assertThat(replicas.writes).containsKeys("k0", "k4", "after-1", "after-2").doesNotContainKey("k5");
    }

    /**
     * Bytes that are not a record must not be trusted as lengths: a garbage length used to size
     * an array before the checksum was looked at, and a large one stopped the node from starting.
     */
    @Test
    void garbageAtTheEndOfAHintFileIsDroppedWithoutBeingTrusted() throws Exception {
        HintedHandoff before = handoff(hintsDir);
        before.store(replica, "k", bytes("v"), 100, 0);
        before.shutdown();
        Path file = hintsDir.resolve("replica.hints");
        java.nio.file.Files.write(file, new byte[] {0x7F, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 1, 2, 3},
                java.nio.file.StandardOpenOption.APPEND); // reads as a 2 GB key length

        HintedHandoff restarted = handoff(hintsDir);
        assertThat(restarted.pendingHints(replica.getId())).isEqualTo(1);
        long size = java.nio.file.Files.size(file);
        restarted.shutdown();
        assertThat(handoff(hintsDir).pendingHints(replica.getId())).isEqualTo(1);
        assertThat(java.nio.file.Files.size(file)).as("the garbage is gone from the file").isEqualTo(size);
    }

    @Test
    void equalVersionsAreCoalescedByTheVersionOrderNotByArrival() throws Exception {
        HintedHandoff handoff = handoff(null);
        handoff.store(replica, "tie", null, 300, 0);
        handoff.store(replica, "tie", bytes("value"), 300, 0);
        handoff.store(replica, "other", bytes("a"), 300, 0);
        handoff.store(replica, "other", bytes("b"), 300, 0);
        handoff.store(replica, "other", bytes("a"), 300, 0);

        recover();
        awaitDelivered(handoff);
        assertThat(replicas.writes.get("tie").value).as("the delete wins the tie").isNull();
        assertThat(new String(replicas.writes.get("other").value, StandardCharsets.UTF_8)).isEqualTo("b");
    }

    /**
     * Hints are kept in memory with their values. They are limited by size too, so a replica that
     * is down for a while under large writes cannot exhaust its coordinators' memory.
     */
    @Test
    void hintsAreLimitedByTheMemoryTheyHold() throws Exception {
        HintedHandoff handoff = handoff(null);
        byte[] large = new byte[8 * 1024 * 1024];
        for (int i = 0; i < 12; i++) {
            handoff.store(replica, "large-" + i, large, 100 + i, 0);
        }
        assertThat(handoff.pendingHints(replica.getId())).isEqualTo(7);
        assertThat(handoff.totalPendingHintBytes()).isLessThanOrEqualTo(HintedHandoff.MAX_HINT_BYTES_PER_NODE);

        // A newer version of a key already held replaces it and is not refused
        handoff.store(replica, "large-0", bytes("small"), 500, 0);
        assertThat(handoff.pendingHints(replica.getId())).isEqualTo(7);
        recover();
        awaitDelivered(handoff);
        assertThat(handoff.totalPendingHintBytes()).isZero();
        assertThat(new String(replicas.writes.get("large-0").value, StandardCharsets.UTF_8)).isEqualTo("small");
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
        awaitHintFile(file -> !java.nio.file.Files.exists(file), "the removed node's hint file deleted");
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
