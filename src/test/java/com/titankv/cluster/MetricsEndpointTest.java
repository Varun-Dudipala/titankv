package com.titankv.cluster;

import com.titankv.TitanKVClient;
import org.junit.jupiter.api.*;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The HTTP endpoints each node serves on its port + 90.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MetricsEndpointTest {

    private static final int BASE_PORT = 20100;
    private final HttpClient http = HttpClient.newHttpClient();
    private TestCluster cluster;

    @BeforeAll
    void startCluster() throws Exception {
        cluster = TestCluster.start(BASE_PORT, 3);
        try (TitanKVClient client = cluster.client()) {
            client.put("metrics-key", "value");
            client.get("metrics-key");
        }
    }

    @AfterAll
    void stopCluster() {
        cluster.close();
    }

    private HttpResponse<String> get(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + (BASE_PORT + 90) + path)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void healthAndReadinessReportOk() throws Exception {
        assertThat(get("/health").statusCode()).isEqualTo(200);
        HttpResponse<String> ready = get("/ready");
        assertThat(ready.statusCode()).isEqualTo(200);
        assertThat(ready.body()).contains("\"ready\"");
    }

    @Test
    void metricsExposeOperationsAndReplicationCounters() throws Exception {
        HttpResponse<String> metrics = get("/metrics");
        assertThat(metrics.statusCode()).isEqualTo(200);
        assertThat(metrics.headers().firstValue("Content-Type").orElse("")).startsWith("text/plain");
        assertThat(metrics.body())
                .contains("# TYPE titankv_ops_total counter")
                .contains("titankv_ops_total{operation=\"put\"}")
                .contains("titankv_latency_seconds_count{operation=\"get\"}")
                .contains("titankv_hints_pending")
                .contains("titankv_read_repairs_total")
                .contains("titankv_antientropy_keys_synced_total")
                .contains("titankv_replica_ops_total{operation=\"write\"}")
                .contains("titankv_cluster_alive_nodes 3");
    }

    @Test
    void statusDescribesTheCluster() throws Exception {
        String status = get("/status").body();
        assertThat(status).contains("\"alive_nodes\": 3").contains("\"ready\": true").contains("node-1");
    }

    @Test
    void unknownPathsAndMethodsAreRejected() throws Exception {
        assertThat(get("/nope").statusCode()).isEqualTo(404);
        HttpResponse<String> post = http.send(HttpRequest.newBuilder(
                        URI.create("http://localhost:" + (BASE_PORT + 90) + "/metrics"))
                .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(post.statusCode()).isEqualTo(405);
    }
}
