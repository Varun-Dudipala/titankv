package com.titankv.cluster;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.DatagramSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The membership a node saves to disk, and how a cluster manager restores it.
 */
class MembershipFileTest {

    @TempDir
    Path dir;

    @Test
    void savedMembershipIsReadBack() throws IOException {
        Path file = dir.resolve("members");
        MembershipFile.Contents saved = new MembershipFile.Contents(
                List.of(new MembershipFile.Member("node-2", "host-2", 9002, 1_700_000_000_000L),
                        new MembershipFile.Member("nœud-3", "::1", 9003, 0)),
                Map.of("node-9", 42L));

        MembershipFile.save(file, saved);

        assertThat(MembershipFile.load(file)).isEqualTo(saved);
        assertThat(dir.resolve("members.tmp")).doesNotExist();
    }

    @Test
    void missingFileIsNotAnError() throws IOException {
        assertThat(MembershipFile.load(dir.resolve("members"))).isNull();
    }

    @Test
    void damagedFileIsRejectedRatherThanHalfRead() throws IOException {
        Path file = dir.resolve("members");
        MembershipFile.save(file, new MembershipFile.Contents(
                List.of(new MembershipFile.Member("node-2", "host-2", 9002, 7)), Map.of()));
        byte[] bytes = Files.readAllBytes(file);
        bytes[12] ^= 0x01;
        Files.write(file, bytes);
        assertThatThrownBy(() -> MembershipFile.load(file)).isInstanceOf(IOException.class)
                .hasMessageContaining("checksum");

        Files.write(file, new byte[] {1, 2, 3});
        assertThatThrownBy(() -> MembershipFile.load(file)).isInstanceOf(IOException.class)
                .hasMessageContaining("truncated");
    }

    @Test
    void restartedClusterManagerHasEveryMemberOnTheRingAndRemembersRemovals() {
        Path file = dir.resolve("members");
        ClusterManager before = new ClusterManager(new Node("a", "localhost", 18101), null, file);
        Node b = new Node("b", "localhost", 18102);
        b.advanceHeartbeat(77, 3);
        before.addNode(b);
        Node c = new Node("c", "localhost", 18103);
        c.advanceHeartbeat(5, 1);
        before.addNode(c);
        c.setStatus(Node.Status.DEAD);
        assertThat(before.removeDeadNode("c")).isNull();

        ClusterManager restarted = new ClusterManager(new Node("a", "localhost", 18101), null, file);

        Node restored = restarted.getNode("b");
        assertThat(restored).isNotNull();
        assertThat(restored.getStatus()).as("not heard from yet").isEqualTo(Node.Status.DEAD);
        assertThat(restored.getGeneration()).isEqualTo(77);
        assertThat(restored.getPort()).isEqualTo(18102);
        assertThat(restarted.getHashRing().getNodeCount()).isEqualTo(2);
        assertThat(restarted.getReplicasForKey("any-key", 3)).contains(restored);
        assertThat(restarted.hasCompleteMembership()).isTrue();

        assertThat(restarted.getNode("c")).as("removed before the restart").isNull();
        assertThat(restarted.hasDeparted("c", 5)).isTrue();
        assertThat(restarted.hasDeparted("c", 6)).as("a later generation of it may rejoin").isFalse();
    }

    @Test
    void damagedMembershipFileIsIgnoredAndRelearned() throws IOException {
        Path file = dir.resolve("members");
        Files.write(file, "not a membership file".getBytes());

        ClusterManager manager = new ClusterManager(new Node("a", "localhost", 18101), null, file);
        assertThat(manager.getNodeCount()).isEqualTo(1);

        manager.addNode(new Node("b", "localhost", 18102));
        assertThat(MembershipFile.load(file).members()).extracting(MembershipFile.Member::id).containsExactly("b");
    }

    @Test
    void membershipIsNotSavedWithoutAFile() {
        ClusterManager manager = new ClusterManager(new Node("a", "localhost", 18101), null, null);
        manager.addNode(new Node("b", "localhost", 18102));
        assertThat(manager.getNodeCount()).isEqualTo(2);
        assertThat(dir).isEmptyDirectory();
    }

    /**
     * A node that cannot open its gossip port could neither join the cluster nor be found, so it
     * must fail to start. It used to log the error and carry on.
     */
    @Test
    void nodeDoesNotStartWithoutItsGossipPort() throws Exception {
        try (DatagramSocket taken = new DatagramSocket(19111)) {
            ClusterManager manager = new ClusterManager(new Node("a", "localhost", 18111), (String) null);

            assertThatThrownBy(() -> manager.start(null)).isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("gossip port 19111");
            assertThat(manager.isRunning()).isFalse();
            assertThat(manager.isReady()).isFalse();
        }
    }
}
