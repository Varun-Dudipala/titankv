package com.titankv.cluster;

import com.titankv.TitanKVClient;
import org.junit.jupiter.api.*;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What the dashboard is for: a node that crashes is seen going SUSPECT and then DEAD, the writes
 * it misses pile up as hints on the node that coordinated them, and when it returns it is ALIVE
 * again and the hints are delivered. All of it read from node 1's HTTP endpoints, as the page does.
 */
@Tag("integration")
class DashboardFailureTest {

    private static final int BASE_PORT = 20600;
    private final HttpClient http = HttpClient.newHttpClient();

    private String get(String path) {
        try {
            return http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + (BASE_PORT + 90) + path)).build(),
                    HttpResponse.BodyHandlers.ofString()).body();
        } catch (Exception e) {
            throw new AssertionError("GET " + path + " failed", e);
        }
    }

    /** The member's entry in node 1's cluster view, up to where its own numbers start. */
    private String member(String id) {
        Matcher matcher = Pattern.compile("\\{\"id\":\"" + id + "\",[^{]*\"self\":(null|\\{\"node\")")
                .matcher(get("/api/cluster"));
        assertThat(matcher.find()).as(id + " in /api/cluster").isTrue();
        return matcher.group();
    }

    private long hintsFor(String target) {
        Matcher matcher = Pattern.compile("\"by_target\":\\{[^}]*\"" + target + "\":(\\d+)").matcher(get("/api/node"));
        return matcher.find() ? Long.parseLong(matcher.group(1)) : 0;
    }

    private void await(java.util.function.BooleanSupplier condition, long timeoutMs, String what) {
        TestCluster.awaitCondition(condition, timeoutMs, what);
    }

    @Test
    void aCrashAndTheRecoveryAreVisible() throws Exception {
        try (TestCluster cluster = TestCluster.start(BASE_PORT, 3)) {
            await(() -> member("node-3").contains("\"status\":\"ALIVE\"") && member("node-3").endsWith("{\"node\""),
                    10_000, "node 3 to be alive and answering");
            assertThat(get("/api/replicas?key=k")).contains("\"replicas_up\":3");

            cluster.crashNode(2);

            // Silent for 3 seconds: suspect. Its own numbers are gone at once
            await(() -> member("node-3").contains("\"status\":\"SUSPECT\""), 8_000, "node 3 to be suspected");
            assertThat(member("node-3")).contains("\"up\":false").endsWith("\"self\":null");
            assertThat(get("/api/replicas?key=k")).contains("\"replicas_up\":2");

            // Writes still succeed at QUORUM; node 1 keeps what node 3 missed
            try (TitanKVClient client = cluster.client(cluster.address(0))) {
                for (int i = 0; i < 25; i++) {
                    client.put("missed-" + i, "value-" + i);
                }
            }
            await(() -> hintsFor("node-3") == 25, 5_000, "node 1 to hold 25 hints for node 3");
            assertThat(get("/api/node")).contains("\"hints\":{\"pending\":25,");

            // Silent for 10 seconds: dead
            await(() -> member("node-3").contains("\"status\":\"DEAD\""), 15_000, "node 3 to be declared dead");
            assertThat(hintsFor("node-3")).isEqualTo(25);

            cluster.startNode(2);

            await(() -> member("node-3").contains("\"status\":\"ALIVE\""), 15_000, "node 3 to be alive again");
            await(() -> hintsFor("node-3") == 0, 30_000, "the hints for node 3 to be delivered");
            await(() -> get("/api/cluster").contains("\"self\":{\"node\":\"node-3\""), 10_000, "node 3 to answer again");
            await(() -> get("/api/cluster").matches(".*\\{\"node\":\"node-3\"[^{]*\"ready\":true.*"),
                    30_000, "node 3 to be ready");
            assertThat(get("/api/replicas?key=k")).contains("\"replicas_up\":3");
            for (int i = 0; i < 25; i++) {
                assertThat(cluster.node(2).getStore().getRaw("missed-" + i)).as("missed-" + i).isPresent();
            }
        }
    }
}
