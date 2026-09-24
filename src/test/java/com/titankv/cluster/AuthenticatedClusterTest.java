package com.titankv.cluster;

import com.titankv.TitanKVClient;
import com.titankv.network.protocol.BinaryProtocol;
import com.titankv.network.protocol.Command;
import com.titankv.network.protocol.Response;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A cluster running with a cluster secret, as required outside dev mode.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AuthenticatedClusterTest {

    private static final int BASE_PORT = 19500;
    private TestCluster cluster;

    @BeforeAll
    void startCluster() throws Exception {
        System.setProperty("titankv.cluster.secret", "integration-test-secret");
        cluster = TestCluster.start(BASE_PORT, 3);
    }

    @AfterAll
    void stopCluster() {
        if (cluster != null) {
            cluster.close();
        }
        System.clearProperty("titankv.cluster.secret");
    }

    @Test
    void quorumReadsAndWritesSucceedThroughEveryNode() throws IOException {
        for (int i = 0; i < cluster.size(); i++) {
            try (TitanKVClient client = cluster.client(cluster.address(i))) {
                String key = "auth-key-" + i;
                client.put(key, "value-" + i);
                assertThat(client.getString(key)).contains("value-" + i);
                assertThat(client.exists(key)).isTrue();
                client.delete(key);
                assertThat(client.getString(key)).isEmpty();
            }
        }
    }

    @Test
    void internalCommandsWithoutTokenAreRejected() throws IOException {
        try (SocketChannel channel = SocketChannel.open(new InetSocketAddress("localhost", BASE_PORT))) {
            Command forged = new Command(Command.PUT_INTERNAL, "forged", "x".getBytes(), System.currentTimeMillis(), 0);
            ByteBuffer out = BinaryProtocol.encode(forged);
            while (out.hasRemaining()) {
                channel.write(out);
            }
            ByteBuffer in = ByteBuffer.allocate(4096);
            do {
                channel.read(in);
                in.flip();
                if (BinaryProtocol.hasCompleteResponse(in)) {
                    break;
                }
                in.compact();
            } while (true);
            Response response = BinaryProtocol.decodeResponse(in);
            assertThat(response.isError()).isTrue();
            assertThat(response.getErrorMessage()).contains("AUTH required");
        }
        assertThat(cluster.node(0).getStore().exists("forged")).isFalse();
    }
}
