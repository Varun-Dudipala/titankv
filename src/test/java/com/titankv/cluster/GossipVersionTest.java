package com.titankv.cluster;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A node drops gossip from a build that speaks another gossip version, instead of reading its
 * bytes as if they were its own format.
 */
class GossipVersionTest {

    private static final int PORT = 18200;

    private ClusterManager manager;

    @AfterEach
    void stop() {
        if (manager != null) {
            manager.stop(false);
        }
    }

    @Test
    void joinFromAnotherGossipVersionIsIgnored() throws Exception {
        manager = new ClusterManager(new Node("local", "localhost", PORT), (String) null);
        manager.start(null);

        send(join((byte) (GossipProtocol.GOSSIP_VERSION - 1), "old-build"));
        send(join((byte) (GossipProtocol.GOSSIP_VERSION + 1), "new-build"));
        send(join(GossipProtocol.GOSSIP_VERSION, "same-build"));

        TestCluster.awaitCondition(() -> manager.getNode("same-build") != null, 5_000,
                "the node with the same gossip version to be learned");
        assertThat(manager.getNode("old-build")).isNull();
        assertThat(manager.getNode("new-build")).isNull();
        assertThat(manager.getNodeCount()).isEqualTo(2);
    }

    /** A JOIN as GossipProtocol writes it, with the given version byte. */
    private static byte[] join(byte version, String senderId) {
        ByteBuffer buffer = ByteBuffer.allocate(256);
        buffer.put(version);
        buffer.put((byte) 0x01); // JOIN
        buffer.putLong(System.currentTimeMillis());
        putString(buffer, senderId);
        putString(buffer, "localhost");
        buffer.putInt(18250);
        buffer.putLong(System.currentTimeMillis()); // generation
        buffer.putLong(1); // heartbeat version
        byte[] bytes = new byte[buffer.position()];
        buffer.flip();
        buffer.get(bytes);
        return bytes;
    }

    private static void putString(ByteBuffer buffer, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        buffer.putShort((short) bytes.length);
        buffer.put(bytes);
    }

    private static void send(byte[] message) throws Exception {
        try (DatagramSocket socket = new DatagramSocket()) {
            socket.send(new DatagramPacket(message, message.length, InetAddress.getByName("localhost"), PORT + 1000));
        }
    }
}
