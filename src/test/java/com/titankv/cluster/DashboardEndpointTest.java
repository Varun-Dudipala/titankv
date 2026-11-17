package com.titankv.cluster;

import com.titankv.TitanKVClient;
import org.junit.jupiter.api.*;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.atIndex;
import static org.assertj.core.api.Assertions.within;

/**
 * The dashboard page and the JSON it reads, served by each node on its port + 90.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DashboardEndpointTest {

    private static final int BASE_PORT = 20500;
    private static final String[] API_PATHS = {
        "/dashboard", "/api/cluster", "/api/node", "/api/ring", "/api/replicas?key=some-key"
    };

    private final HttpClient http = HttpClient.newHttpClient();
    private TestCluster cluster;

    @BeforeAll
    void startCluster() throws Exception {
        cluster = TestCluster.start(BASE_PORT, 3);
        try (TitanKVClient client = cluster.client(cluster.address(0))) {
            for (int i = 0; i < 20; i++) {
                client.put("dashboard-key-" + i, "value-" + i);
            }
            client.get("dashboard-key-0");
            client.get("no-such-key");
        }
    }

    @AfterAll
    void stopCluster() {
        cluster.close();
    }

    private HttpResponse<String> get(int node, String path) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + (BASE_PORT + node + 90) + path)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> get(String path) throws Exception {
        return get(0, path);
    }

    /** Every match of the pattern's first group, in order. */
    private static List<String> all(String text, String regex) {
        List<String> found = new ArrayList<>();
        Matcher matcher = Pattern.compile(regex).matcher(text);
        while (matcher.find()) {
            found.add(matcher.group(1));
        }
        return found;
    }

    private static long number(String json, String field) {
        List<String> values = all(json, "\"" + field + "\":(-?\\d+)");
        assertThat(values).as(field + " in " + json).isNotEmpty();
        return Long.parseLong(values.get(0));
    }

    @Test
    void dashboardIsOneSelfContainedPage() throws Exception {
        HttpResponse<String> page = get("/dashboard");

        assertThat(page.statusCode()).isEqualTo(200);
        assertThat(page.headers().firstValue("Content-Type").orElse("")).startsWith("text/html");
        // The browser is told to load nothing from anywhere else, and the page asks for nothing
        assertThat(page.headers().firstValue("Content-Security-Policy").orElse(""))
                .contains("default-src 'none'").contains("connect-src 'self'");
        String html = page.body().replace("http://www.w3.org/2000/svg", ""); // a namespace, not a request
        assertThat(html).contains("<title>TitanKV dashboard</title>").contains("/api/cluster");
        assertThat(html).doesNotContain("http://").doesNotContain("https://").doesNotContain("//cdn")
                .doesNotContain("<link").doesNotContain("@import").doesNotContain("url(")
                .doesNotContain(" src=");
    }

    @Test
    void nodeReportsItsOwnNumbersAndItsViewOfTheMembers() throws Exception {
        HttpResponse<String> response = get("/api/node");
        String node = response.body();

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.headers().firstValue("Content-Type").orElse("")).startsWith("application/json");
        assertThat(node).startsWith("{\"node\":\"node-1\"")
                .contains("\"ready\":true").contains("\"catching_up\":false").contains("\"healthy\":true")
                .contains("\"wal_enabled\":false") // tests run in dev mode
                .contains("\"get_p99\":").contains("\"put_p99\":").contains("\"memory_bytes\":");
        // Every write went through node 1, which coordinated them; the other nodes took replica writes
        assertThat(number(node, "puts")).isEqualTo(20);
        assertThat(number(node, "gets")).isEqualTo(2);
        assertThat(number(node, "keys")).isEqualTo(20);
        assertThat(number(node, "pending")).isZero();
        String other = get(1, "/api/node").body();
        assertThat(other).startsWith("{\"node\":\"node-2\"");
        assertThat(number(other, "replica_writes")).isPositive();
        assertThat(number(other, "puts")).isZero();

        assertThat(all(node, "\\{\"id\":\"(node-\\d)\",\"address\":\"[^\"]+\",\"status\":\"ALIVE\",\"up\":true"))
                .containsExactly("node-1", "node-2", "node-3");
        assertThat(all(node, "\"id\":\"(node-\\d)\",[^}]*\"local\":true")).containsExactly("node-1");
    }

    @Test
    void clusterCarriesEveryNodesOwnNumbers() throws Exception {
        // The serving node asks the other two over HTTP; give a slow machine more than one try
        TestCluster.awaitCondition(() -> {
            try {
                return all(get("/api/cluster").body(), "\"self\":\\{\"node\":\"(node-\\d)\"").size() == 3;
            } catch (Exception e) {
                return false;
            }
        }, 10_000, "/api/cluster to carry all three nodes' own numbers");

        String viewOfNode1 = get(0, "/api/cluster").body();
        assertThat(viewOfNode1).contains("\"served_by\":\"node-1\"").contains("\"replication_factor\":3");
        assertThat(all(viewOfNode1, "\"self\":\\{\"node\":\"(node-\\d)\"")).containsExactly("node-1", "node-2", "node-3");
        // Each "self" is what that node says about itself: only node 1 coordinated the writes
        assertThat(all(viewOfNode1, "\"puts\":(\\d+)")).containsExactly("20", "0", "0");
        assertThat(all(viewOfNode1, "\"replica_writes\":(\\d+)")).hasSize(3).startsWith("0").doesNotContain("0", atIndex(1))
                .doesNotContain("0", atIndex(2));

        // Any node can serve the page, each naming itself as the one whose view it is
        TestCluster.awaitCondition(() -> {
            try {
                return all(get(2, "/api/cluster").body(), "\"self\":\\{\"node\":\"(node-\\d)\"").size() == 3;
            } catch (Exception e) {
                return false;
            }
        }, 10_000, "node 3 to carry all three nodes' own numbers");
        assertThat(get(2, "/api/cluster").body()).contains("\"served_by\":\"node-3\"");
    }

    @Test
    void replicasAreTheOnesTheRingGives() throws Exception {
        String[] keys = {"user:42", "dashboard-key-7", "a b&c=d", "100%", "ключ/é?#", "\"quoted\"\\"};
        for (String key : keys) {
            HttpResponse<String> response = get("/api/replicas?key=" + URLEncoder.encode(key, StandardCharsets.UTF_8));
            assertThat(response.statusCode()).as(key).isEqualTo(200);
            String body = response.body();

            List<String> expected = cluster.node(0).getClusterManager().getReplicasForKey(key, 3)
                    .stream().map(Node::getId).toList();
            String replicas = body.substring(body.indexOf("\"replicas\":["));
            assertThat(all(replicas, "\"id\":\"(node-\\d)\"")).as(key).isEqualTo(expected).hasSize(3);
            assertThat(body).as(key)
                    .contains("\"hash\":\"" + com.titankv.util.MurmurHash3.hash64(key) + "\"")
                    .contains("\"replication_factor\":3").contains("\"quorum\":2").contains("\"replicas_up\":3");
        }
        // The key comes back as it was typed, escaped so that it stays inside its string
        assertThat(get("/api/replicas?key=" + URLEncoder.encode("\"quoted\"\\", StandardCharsets.UTF_8)).body())
                .startsWith("{\"key\":\"\\\"quoted\\\"\\\\\",");
        assertThat(get("/api/replicas?key=a+b%26c%3Dd").body()).startsWith("{\"key\":\"a b&c=d\",");
    }

    @Test
    void replicasNeedsAKey() throws Exception {
        assertThat(get("/api/replicas").statusCode()).isEqualTo(400);
        assertThat(get("/api/replicas?key=").statusCode()).isEqualTo(400);
        assertThat(get("/api/replicas?other=1").statusCode()).isEqualTo(400);
        // Percent-encoding that decodes to nothing; the Java client refuses to send it, so by hand
        try (java.net.Socket socket = new java.net.Socket("localhost", BASE_PORT + 90)) {
            socket.getOutputStream().write("GET /api/replicas?key=%zz HTTP/1.1\r\nHost: localhost\r\n\r\n"
                    .getBytes(StandardCharsets.US_ASCII));
            String reply = new String(socket.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            assertThat(reply).startsWith("HTTP/1.1 400 ");
        }
        assertThat(get("/api/replicas?key=" + "k".repeat(64 * 1024 + 1)).statusCode()).isEqualTo(400);
    }

    @Test
    void ringCoversTheWholeHashSpace() throws Exception {
        String ring = get("/api/ring").body();

        assertThat(number(ring, "virtual_nodes")).isEqualTo(450);
        assertThat(all(ring, "\\{\"id\":\"(node-\\d)\",\"virtual_nodes\":150,")).containsExactly("node-1", "node-2", "node-3");
        double total = all(ring, "\"share\":([0-9.]+)").stream().mapToDouble(Double::parseDouble).sum();
        assertThat(total).isCloseTo(1.0, within(0.001));

        List<String> vnodes = all(ring.substring(ring.indexOf("\"vnodes\":")), "\\[([0-9.]+),\\d\\]");
        assertThat(vnodes).hasSize(450);
        List<Double> positions = vnodes.stream().map(Double::parseDouble).toList();
        assertThat(positions).isSorted().allMatch(p -> p >= 0.0 && p <= 1.0);

        // Each range belongs to the node the ring itself would pick for a hash inside it
        Matcher first = Pattern.compile("\"vnodes\":\\[\\[([0-9.]+),(\\d)\\]").matcher(ring);
        assertThat(first.find()).isTrue();
        ConsistentHash hashRing = cluster.node(0).getClusterManager().getHashRing();
        VirtualNode lowest = hashRing.getVirtualNodes().get(0);
        assertThat(Double.parseDouble(first.group(1)))
                .isCloseTo(ConsistentHash.positionOf(lowest.getHashValue()), within(1e-6));
        assertThat("node-" + (Integer.parseInt(first.group(2)) + 1)).isEqualTo(lowest.getPhysicalNode().getId());
    }

    @Test
    void endpointsOnlyAcceptGet() throws Exception {
        for (String path : API_PATHS) {
            for (String method : new String[] {"POST", "PUT", "DELETE", "PATCH"}) {
                HttpResponse<String> response = http.send(HttpRequest.newBuilder(
                                URI.create("http://localhost:" + (BASE_PORT + 90) + path))
                        .method(method, HttpRequest.BodyPublishers.ofString("{\"status\":\"DEAD\"}")).build(),
                        HttpResponse.BodyHandlers.ofString());
                assertThat(response.statusCode()).as(method + " " + path).isEqualTo(405);
            }
        }
    }

    @Test
    void readingTheDashboardChangesNothing() throws Exception {
        List<String> before = snapshot();

        for (int round = 0; round < 3; round++) {
            for (int node = 0; node < cluster.size(); node++) {
                for (String path : API_PATHS) {
                    assertThat(get(node, path).statusCode()).as(path).isEqualTo(200);
                }
                get(node, "/api/replicas?key=never-written-" + round);
            }
        }

        assertThat(snapshot()).isEqualTo(before);
        for (int node = 0; node < cluster.size(); node++) {
            assertThat(cluster.node(node).getStore().getRaw("some-key")).isEmpty();
            assertThat(cluster.node(node).getStore().getRaw("never-written-0")).isEmpty();
        }
    }

    /** What the endpoints must leave alone: every node's entries and every node's member list. */
    private List<String> snapshot() {
        List<String> state = new ArrayList<>();
        for (int node = 0; node < cluster.size(); node++) {
            var server = cluster.node(node);
            state.add("node " + node + " entries " + new java.util.TreeSet<>(
                    ((com.titankv.core.InMemoryStore) server.getStore()).keysIncludingTombstones()));
            state.add("node " + node + " members " + server.getClusterManager().getAllNodes().stream()
                    .map(member -> member.getId() + "=" + member.getStatus() + "/" + member.getGeneration())
                    .sorted().toList());
            state.add("node " + node + " ring " + server.getClusterManager().getHashRing().getVirtualNodeCount());
        }
        return state;
    }
}
