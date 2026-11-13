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
     * A write that starts after another client's write was acknowledged must win, even when its
     * client's clock is 20 seconds behind the other's and it has never seen that write. Its
     * version is too low; a replica holding the newer version says so, and the client sends the
     * write again with a version after it.
     */
    @Test
    void laterWriteFromAClientWithASlowClockStillWins() throws Exception {
        try (TestCluster cluster = TestCluster.start(BASE_PORT + 10, 3);
             TitanKVClient ahead = cluster.client();
             TitanKVClient behind = cluster.client()) {
            ahead.setClockSkewMillis(10_000);
            behind.setClockSkewMillis(-10_000);

            ahead.put("client-skew", "v1");
            behind.put("client-skew", "v2");
            assertThat(ahead.getString("client-skew")).contains("v2");

            ahead.put("client-skew", "v3");
            behind.delete("client-skew");
            assertThat(ahead.getString("client-skew")).isEmpty();
            assertThat(ahead.exists("client-skew")).isFalse();

            ahead.put("client-skew", "v4");
            behind.put("client-skew", "v5", ConsistencyLevel.ALL);
            assertThat(ahead.getString("client-skew", ConsistencyLevel.ALL)).contains("v5");
            cluster.awaitReplicated("client-skew");
            for (int i = 0; i < 3; i++) {
                assertThat(new String(cluster.node(i).getStore().get("client-skew").orElseThrow().getValueUnsafe(),
                        StandardCharsets.UTF_8)).as("node " + (i + 1)).isEqualTo("v5");
            }
        }
    }

    /**
     * Four clients with clocks up to 20 seconds apart take turns writing one key as fast as they
     * can, so most writes follow another client's within the same millisecond. Each write is
     * acknowledged before the next starts, so each must be the one a read then returns.
     */
    @Test
    void writesTakingTurnsBetweenClientsKeepTheirOrder() throws Exception {
        try (TestCluster cluster = TestCluster.start(BASE_PORT + 50, 3);
             TitanKVClient a = cluster.client(cluster.address(0));
             TitanKVClient b = cluster.client(cluster.address(1));
             TitanKVClient c = cluster.client(cluster.address(2));
             TitanKVClient d = cluster.client()) {
            b.setClockSkewMillis(-10_000);
            c.setClockSkewMillis(10_000);
            d.setClockSkewMillis(-3);
            TitanKVClient[] clients = {a, b, c, d};

            for (int turn = 0; turn < 400; turn++) {
                TitanKVClient writer = clients[turn % 4];
                TitanKVClient reader = clients[(turn + 1) % 4];
                if (turn % 10 == 9) {
                    writer.delete("turns");
                    assertThat(reader.getString("turns")).as("after delete in turn " + turn).isEmpty();
                } else {
                    writer.put("turns", "turn-" + turn);
                    assertThat(reader.getString("turns")).as("after turn " + turn).contains("turn-" + turn);
                }
            }
        }
    }

    /**
     * Writers that race on one key are all acknowledged, and every replica ends with the same one
     * of their values: last write wins, with no error and no divergence.
     */
    @Test
    void racingWritersOnOneKeyConvergeOnOneValue() throws Exception {
        try (TestCluster cluster = TestCluster.start(BASE_PORT + 60, 3)) {
            java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(6);
            java.util.List<java.util.concurrent.Future<?>> writers = new java.util.ArrayList<>();
            for (int w = 0; w < 6; w++) {
                int writer = w;
                writers.add(pool.submit(() -> {
                    try (TitanKVClient client = cluster.client()) {
                        client.setClockSkewMillis((writer - 3) * 4_000L);
                        for (int i = 0; i < 150; i++) {
                            client.put("raced", "writer-" + writer + "-" + i);
                        }
                    }
                    return null;
                }));
            }
            for (java.util.concurrent.Future<?> writer : writers) {
                writer.get(60, java.util.concurrent.TimeUnit.SECONDS);
            }
            pool.shutdown();

            TestCluster.awaitCondition(() -> {
                String first = new String(cluster.node(0).getStore().get("raced").orElseThrow().getValueUnsafe(),
                        StandardCharsets.UTF_8);
                for (int i = 1; i < 3; i++) {
                    if (!cluster.node(i).getStore().get("raced").map(kv -> new String(kv.getValueUnsafe(),
                            StandardCharsets.UTF_8)).orElse("").equals(first)) {
                        return false;
                    }
                }
                return first.endsWith("-149");
            }, 10_000, "all replicas to hold the same final write of one of the writers");
        }
    }

    /**
     * A client 58 seconds ahead is inside what nodes accept. Its versions must stay its own
     * problem. When clients adopted every version they were shown, a correct client that read
     * this client's key moved its own clock 58 seconds ahead, and then every write it sent, to
     * any key, was refused by a node whose clock ran 3 seconds slow.
     */
    @Test
    void oneClientsFastClockDoesNotSpreadToOtherClients() throws Exception {
        try (TestCluster cluster = TestCluster.start(BASE_PORT + 70, 3);
             TitanKVClient fast = cluster.client(cluster.address(0));
             TitanKVClient correct = cluster.client(cluster.address(1))) {
            cluster.node(1).getClusterManager().getClock().setSkewMillis(-3_000);
            fast.setClockSkewMillis(58_500);
            fast.put("from-the-future", "written-by-the-fast-client");

            assertThat(correct.getString("from-the-future")).contains("written-by-the-fast-client");
            correct.put("unrelated", "x");
            assertThat(correct.getString("unrelated")).contains("x");

            // The key the fast client wrote cannot be overwritten yet, and the client is told why
            assertThatThrownBy(() -> correct.put("from-the-future", "overwrite"))
                    .isInstanceOf(IOException.class).hasMessageContaining("ahead of this client's clock");
            assertThatThrownBy(() -> correct.delete("from-the-future"))
                    .isInstanceOf(IOException.class).hasMessageContaining("ahead of this client's clock");
            assertThat(correct.observeCausalContext(fast.getCausalContext())).isFalse();
            correct.put("unrelated", "y");
            assertThat(correct.getString("unrelated")).contains("y");
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

            // A write with a version older than the key's is taken but will never be read:
            // the reply says so, with the version it lost to
            Response stale = send(socket, new Command(Command.PUT, "raw-key",
                    "too-old".getBytes(StandardCharsets.UTF_8), clientVersion, 0));
            assertThat(stale.getStatus()).isEqualTo(Response.STALE);
            assertThat(stale.getTimestamp()).isEqualTo(delete.getTimestamp());
            assertThat(client.getString("raw-key")).isEmpty();
        }
    }

    static Response send(Socket socket, Command command) throws IOException {
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
