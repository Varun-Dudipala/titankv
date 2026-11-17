package com.titankv.cluster;

import com.titankv.TitanKVClient;
import com.titankv.consistency.ConsistencyLevel;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A cluster with fewer nodes than the replication factor keeps fewer copies, and the consistency
 * levels are counted against those. The dashboard used to count against the replication factor,
 * and told a healthy single node that QUORUM could not be met.
 */
@Tag("integration")
class DashboardSmallClusterTest {

    private static final int BASE_PORT = 20700;

    private final HttpClient http = HttpClient.newHttpClient();

    private String get(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + (BASE_PORT + 90) + path)).build(),
                HttpResponse.BodyHandlers.ofString()).body();
    }

    @Test
    void theDashboardAgreesWithWhatASmallClusterAccepts() throws Exception {
        try (TestCluster cluster = TestCluster.start(BASE_PORT, 1);
             TitanKVClient client = cluster.client(cluster.address(0))) {
            // One node takes writes and reads at every level
            client.put("solo", "value".getBytes(), 0, ConsistencyLevel.ALL);
            client.put("solo", "value".getBytes(), 0, ConsistencyLevel.QUORUM);
            assertThat(client.getString("solo", ConsistencyLevel.ALL)).contains("value");

            assertThat(get("/api/replicas?key=solo")).contains("\"replication_factor\":3").contains("\"copies\":1")
                    .contains("\"quorum\":1").contains("\"replicas_up\":1");
            assertThat(get("/api/cluster")).contains("\"replication_factor\":3").contains("\"copies\":1")
                    .contains("\"quorum\":1");

            cluster.addNode();
            cluster.awaitAliveCount(0, 2, 20_000);
            TestCluster.awaitCondition(() -> {
                try {
                    return get("/api/replicas?key=solo").contains("\"replicas_up\":2");
                } catch (Exception e) {
                    return false;
                }
            }, 10_000, "both nodes to be replicas of the key");

            assertThat(get("/api/replicas?key=solo")).contains("\"copies\":2").contains("\"quorum\":2");
            assertThat(get("/api/cluster")).contains("\"copies\":2").contains("\"quorum\":2");
        }
    }
}
