package com.titankv.cluster;

import com.titankv.TitanKVClient;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Time-to-live set by the client, on a single node and replicated across a cluster.
 */
@Tag("integration")
class TtlTest {

    private static final int BASE_PORT = 19850;

    @Test
    void singleNodeExpiresKeys() throws Exception {
        try (TestCluster cluster = TestCluster.start(BASE_PORT, 1);
             TitanKVClient client = cluster.client()) {
            assertExpires(client);
        }
    }

    @Test
    void clusterExpiresKeysOnEveryReplica() throws Exception {
        try (TestCluster cluster = TestCluster.start(BASE_PORT + 10, 3);
             TitanKVClient client = cluster.client()) {
            assertExpires(client);
            for (int i = 0; i < 3; i++) {
                assertThat(cluster.node(i).getStore().getRaw("ttl-key")).isEmpty();
            }
        }
    }

    private static void assertExpires(TitanKVClient client) throws Exception {
        client.put("ttl-key", "short-lived".getBytes(StandardCharsets.UTF_8), 500);
        client.put("durable-key", "stays");

        assertThat(client.getString("ttl-key")).contains("short-lived");
        assertThat(client.getWithMetadata("ttl-key").orElseThrow().getExpiresAt())
                .isGreaterThan(System.currentTimeMillis());

        Thread.sleep(800);
        assertThat(client.getString("ttl-key")).isEmpty();
        assertThat(client.exists("ttl-key")).isFalse();
        assertThat(client.getString("durable-key")).contains("stays");
    }
}
