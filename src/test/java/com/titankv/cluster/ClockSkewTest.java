package com.titankv.cluster;

import com.titankv.TitanKVClient;
import com.titankv.consistency.ConsistencyLevel;
import com.titankv.network.protocol.BinaryProtocol;
import com.titankv.network.protocol.Command;
import com.titankv.network.protocol.Response;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Last write wins by version, and the client chooses each write's version from its own hybrid
 * logical clock. These tests run node clocks and client clocks wrong on purpose.
 */
@Tag("integration")
class ClockSkewTest {

    private static final int BASE_PORT = 20200;

    @Test
    void nodeClocksDoNotDecideWhichWriteWins() throws Exception {
        try (TestCluster cluster = TestCluster.start(BASE_PORT, 3);
             TitanKVClient viaNode1 = cluster.client(cluster.address(0));
             TitanKVClient viaSlowNode = cluster.client(cluster.address(1));
             TitanKVClient viaFastNode = cluster.client(cluster.address(2))) {
            cluster.node(1).getClusterManager().getClock().setSkewMillis(-30_000);
            cluster.node(2).getClusterManager().getClock().setSkewMillis(20_000);

            viaFastNode.put("skew-key", "first");
            viaSlowNode.put("skew-key", "second");
            assertThat(viaNode1.getString("skew-key")).contains("second");

            viaNode1.put("skew-key", "third");
            viaSlowNode.put("skew-key", "fourth");
            viaFastNode.delete("skew-key");
            assertThat(viaNode1.getString("skew-key", ConsistencyLevel.ALL)).isEmpty();
            viaSlowNode.put("skew-key", "fifth");
            assertThat(viaFastNode.getString("skew-key", ConsistencyLevel.ALL)).contains("fifth");
        }
    }

    /**
     * A write that fails for lack of a quorum can stay on its coordinator. When the client's next
     * write is acknowledged through another node, that later write must still win once the first
     * node's copy reaches the other replicas.
     *
     * This failed when the coordinator chose the version: the client never learned the version
     * of the write that failed, so nothing ordered its next write after it, and with the first
     * node's clock 20 seconds ahead the failed write won everywhere.
     */
    @Test
    void acknowledgedWriteIsNotOverriddenByTheClientsEarlierFailedWrite() throws Exception {
        try (TestCluster cluster = TestCluster.start(BASE_PORT + 20, 3);
             TitanKVClient viaFastNode = cluster.client(cluster.address(0));
             TitanKVClient viaOtherNode = cluster.client(cluster.address(1))) {
            cluster.node(0).getClusterManager().getClock().setSkewMillis(20_000);

            cluster.isolate(0);
            // Node 1 has not noticed yet: it writes its own copy, then cannot reach a quorum
            assertThatThrownBy(() -> viaFastNode.put("failover-key", "failed")).isInstanceOf(IOException.class);
            assertThat(cluster.node(0).getStore().get("failover-key")).isPresent();

            // The same client, now talking to node 2, with everything it has done and seen so far
            viaOtherNode.observeCausalContext(viaFastNode.getCausalContext());
            viaOtherNode.put("failover-key", "acknowledged");

            cluster.heal();
            cluster.awaitConverged(20_000);
            TestCluster.awaitCondition(() -> cluster.node(0).getReplicationManager().getHintedHandoff()
                    .totalPendingHints() == 0, 15_000, "node 1 to deliver the failed write's hints");
            assertThat(viaOtherNode.getString("failover-key", ConsistencyLevel.ALL)).contains("acknowledged");
            for (int i = 0; i < 3; i++) {
                assertThat(new String(cluster.node(i).getStore().get("failover-key").orElseThrow().getValueUnsafe(),
                        StandardCharsets.UTF_8)).as("node " + (i + 1)).isEqualTo("acknowledged");
            }
        }
    }

    /**
     * Client clocks decide between writes that know nothing of each other, so the first write of
     * a client whose clock is behind can lose to a value already there. Every reply moves the
     * client's clock past the newest version the node has seen, and every read past the version
     * read, so from then on its writes win.
     */
    @Test
    void clientWithASlowClockCatchesUpFromTheRepliesItGets() throws Exception {
        try (TestCluster cluster = TestCluster.start(BASE_PORT + 10, 3);
             TitanKVClient ahead = cluster.client();
             TitanKVClient behind = cluster.client();
             TitanKVClient alsoBehind = cluster.client()) {
            ahead.setClockSkewMillis(15_000);
            behind.setClockSkewMillis(-15_000);
            alsoBehind.setClockSkewMillis(-15_000);
            ahead.put("client-skew", "v1", ConsistencyLevel.ALL);

            // Knows nothing of v1 and its clock says 30 seconds earlier: last write wins, and this is not it
            behind.put("client-skew", "lost-to-skew", ConsistencyLevel.ALL);
            assertThat(ahead.getString("client-skew", ConsistencyLevel.ALL)).contains("v1");

            // The reply to that write told it how far the cluster's versions have got
            behind.put("client-skew", "v2", ConsistencyLevel.ALL);
            assertThat(ahead.getString("client-skew", ConsistencyLevel.ALL)).contains("v2");

            // A client that reads a value writes after it, however slow its clock
            ahead.put("client-skew", "v3", ConsistencyLevel.ALL);
            assertThat(alsoBehind.getString("client-skew")).contains("v3");
            alsoBehind.put("client-skew", "v4", ConsistencyLevel.ALL);
            assertThat(ahead.getString("client-skew", ConsistencyLevel.ALL)).contains("v4");
            alsoBehind.delete("client-skew", ConsistencyLevel.ALL);
            assertThat(ahead.getString("client-skew", ConsistencyLevel.ALL)).isEmpty();
        }
    }

    /**
     * A version far in the future would outrank every write to the key until real time caught
     * up, so a node refuses one more than a minute ahead of its own clock.
     */
    @Test
    void versionFarAheadOfTheNodesClockIsRefused() throws Exception {
        try (TestCluster cluster = TestCluster.start(BASE_PORT + 30, 1);
             TitanKVClient wrongClock = cluster.client();
             TitanKVClient client = cluster.client()) {
            wrongClock.setClockSkewMillis(10 * 60_000);

            assertThatThrownBy(() -> wrongClock.put("future-key", "from-the-future"))
                    .isInstanceOf(IOException.class).hasMessageContaining("ahead of this node's clock");
            assertThatThrownBy(() -> wrongClock.delete("future-key"))
                    .isInstanceOf(IOException.class).hasMessageContaining("ahead of this node's clock");
            assertThat(cluster.node(0).getStore().getRaw("future-key")).isEmpty();

            client.put("future-key", "on-time");
            assertThat(client.getString("future-key")).contains("on-time");
        }
    }

    /**
     * The wire protocol allows a write without a version (timestamp 0); the node then issues one
     * from its own clock, newer than its copy of the key.
     */
    @Test
    void writeWithoutAVersionGetsOneFromTheNode() throws Exception {
        try (TestCluster cluster = TestCluster.start(BASE_PORT + 40, 1);
             TitanKVClient client = cluster.client();
             Socket socket = new Socket("localhost", BASE_PORT + 40)) {
            client.put("raw-key", "from-the-client");
            long clientVersion = client.getWithMetadata("raw-key").orElseThrow().getTimestamp();

            Response put = send(socket, Command.put("raw-key", "raw".getBytes(StandardCharsets.UTF_8)));
            assertThat(put.isOk()).isTrue();
            assertThat(put.getTimestamp()).isGreaterThan(clientVersion);
            assertThat(client.getString("raw-key")).contains("raw");

            Response delete = send(socket, Command.delete("raw-key"));
            assertThat(delete.isOk()).isTrue();
            assertThat(delete.getTimestamp()).isGreaterThan(put.getTimestamp());
            assertThat(client.getString("raw-key")).isEmpty();
        }
    }

    private static Response send(Socket socket, Command command) throws IOException {
        ByteBuffer request = BinaryProtocol.encode(command);
        socket.getOutputStream().write(request.array(), 0, request.remaining());
        socket.getOutputStream().flush();
        DataInputStream in = new DataInputStream(socket.getInputStream());
        byte[] header = new byte[BinaryProtocol.RESPONSE_HEADER_SIZE];
        in.readFully(header);
        int valueLength = Math.max(0, ByteBuffer.wrap(header).getInt(5));
        byte[] frame = new byte[header.length + valueLength];
        System.arraycopy(header, 0, frame, 0, header.length);
        in.readFully(frame, header.length, valueLength);
        return BinaryProtocol.decodeResponse(ByteBuffer.wrap(frame));
    }
}
