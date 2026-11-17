package com.titankv.util;

import com.sun.net.httpserver.HttpServer;
import com.titankv.cluster.ClusterManager;
import com.titankv.cluster.Node;
import com.titankv.core.InMemoryStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What the node serving the dashboard does with the other nodes' answers. It finds them on ports
 * it has to guess, and passes what they say on inside its own JSON, so it must not pass on text
 * that is not an object, nor an answer that came from some other node.
 */
class DashboardPeerTest {

    private static final int LOCAL_PORT = 18600;   // this node's client port; nothing listens on it
    private static final int HTTP_PORT = 18690;    // its HTTP port, so the others are expected 90 above theirs

    private final List<HttpServer> servers = new ArrayList<>();
    private final Map<Integer, String> bodies = new ConcurrentHashMap<>();
    private InMemoryStore store;
    private ClusterManager cluster;
    private DashboardData dashboard;

    @BeforeEach
    void setUp() {
        store = new InMemoryStore();
        cluster = new ClusterManager(new Node("local", "localhost", LOCAL_PORT));
        dashboard = new DashboardData(HTTP_PORT, new MetricsCollector(), cluster, store, null);
    }

    @AfterEach
    void tearDown() {
        dashboard.close();
        servers.forEach(server -> server.stop(0));
        store.shutdown();
    }

    /** Something answering GET /api/node on the port with whatever body is set for it. */
    private void listen(int port) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("localhost", port), 0);
        server.createContext("/api/node", exchange -> {
            byte[] body = bodies.getOrDefault(port, "").getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        servers.add(server);
    }

    private static String nodeJson(String id) {
        return "{\"node\":\"" + id + "\",\"ready\":true,\"ops\":{\"gets\":7},\"hints\":{\"pending\":0,\"by_target\":{}}}";
    }

    /** The cluster view once the peer's answer (or its absence) has had time to arrive. */
    private String clusterView(String expected) throws Exception {
        String view = dashboard.clusterJson();
        long deadline = System.currentTimeMillis() + 5_000;
        while (!view.contains(expected) && System.currentTimeMillis() < deadline) {
            Thread.sleep(100);
            view = dashboard.clusterJson();
        }
        assertThat(JsonCheck.isObject(view)).as("the cluster view is one JSON object: " + view).isTrue();
        return view;
    }

    @Test
    void aPeersOwnNumbersArePassedOn() throws Exception {
        cluster.addNode(new Node("peer-1", "localhost", LOCAL_PORT + 1));
        listen(LOCAL_PORT + 1 + 90);
        bodies.put(LOCAL_PORT + 1 + 90, nodeJson("peer-1"));

        String view = clusterView("\"self\":{\"node\":\"peer-1\"");

        assertThat(view).contains("\"self\":{\"node\":\"peer-1\",\"ready\":true,\"ops\":{\"gets\":7}");
    }

    @Test
    void textThatIsNotOneJsonObjectIsNotPassedOn() throws Exception {
        cluster.addNode(new Node("peer-1", "localhost", LOCAL_PORT + 1));
        listen(LOCAL_PORT + 1 + 90);
        String[] bodiesThatLookRightAtBothEnds = {
            "{\"node\":\"peer-1\"},\"injected\":{\"x\":1}",
            "{\"node\":\"peer-1\",\"ops\":{\"gets\":7}",
            "{\"node\":\"peer-1\"}{\"again\":1}",
            "{\"node\":\"peer-1\",oops}",
        };
        for (String body : bodiesThatLookRightAtBothEnds) {
            bodies.put(LOCAL_PORT + 1 + 90, nodeJson("peer-1"));
            clusterView("\"self\":{\"node\":\"peer-1\"");
            bodies.put(LOCAL_PORT + 1 + 90, body);

            String view = clusterView("\"self\":null"); // only the peer's: this node always has its own

            assertThat(view).as(body).doesNotContain("injected").doesNotContain("again").doesNotContain("oops");
            assertThat(view).as(body).containsPattern("\"id\":\"peer-1\"[^{]*\"self\":null");
        }
    }

    /**
     * The second port tried is this node's own HTTP port on the member's host. On one machine
     * that is this node itself, which answers with its own numbers: they are not the member's.
     */
    @Test
    void anAnswerFromAnotherNodeIsNotTakenForTheMembers() throws Exception {
        cluster.addNode(new Node("peer-1", "localhost", LOCAL_PORT + 1)); // nothing listens 90 above it
        listen(HTTP_PORT);
        bodies.put(HTTP_PORT, nodeJson("local"));

        Thread.sleep(600); // long enough for the peer to have been asked on both ports
        dashboard.clusterJson();
        Thread.sleep(600);
        String view = clusterView("\"id\":\"peer-1\"");

        assertThat(view).containsPattern("\"id\":\"peer-1\"[^{]*\"self\":null");
        assertThat(view).doesNotContain("\"gets\":7");
    }

    /** Nodes on separate machines, all given one HTTP port: not 90 above the client port. */
    @Test
    void aPeerOnThisNodesOwnHttpPortIsFound() throws Exception {
        cluster.addNode(new Node("peer-1", "localhost", LOCAL_PORT + 1));
        listen(HTTP_PORT);
        bodies.put(HTTP_PORT, nodeJson("peer-1"));

        String view = clusterView("\"self\":{\"node\":\"peer-1\"");

        assertThat(view).contains("\"self\":{\"node\":\"peer-1\",\"ready\":true");
    }

    @Test
    void portsAreTriedInOrderOfLikelihood() {
        // last answered on, then the same distance from the client port, then this node's own port
        assertThat(DashboardData.candidatePorts(0, 9002, 90, 9091)).containsExactly(9092, 9091);
        assertThat(DashboardData.candidatePorts(9100, 9002, 90, 9091)).containsExactly(9100, 9092, 9091);
        assertThat(DashboardData.candidatePorts(9092, 9002, 90, 9091)).containsExactly(9092, 9091);
        // separate machines, one fixed HTTP port and one client port: both guesses are the same port
        assertThat(DashboardData.candidatePorts(0, 9001, 99, 9100)).containsExactly(9100);
        // an HTTP port that was never chosen (0) gives no port worth trying
        assertThat(DashboardData.candidatePorts(0, 9002, -9001, 0)).containsExactly(1);
        assertThat(DashboardData.candidatePorts(0, 9001, -9001, 0)).isEmpty();
        assertThat(DashboardData.candidatePorts(0, 65000, 1000, 9091)).containsExactly(9091);
    }

    @Test
    void aSmallClusterKeepsFewerCopiesThanTheReplicationFactor() {
        // one member: one copy, and one acknowledgement is a quorum
        assertThat(dashboard.replicasJson("k")).contains("\"replication_factor\":3").contains("\"copies\":1")
                .contains("\"quorum\":1").contains("\"replicas_up\":1");
        assertThat(dashboard.clusterJson()).contains("\"copies\":1").contains("\"quorum\":1");

        cluster.addNode(new Node("peer-1", "localhost", LOCAL_PORT + 1));
        assertThat(dashboard.replicasJson("k")).contains("\"copies\":2").contains("\"quorum\":2");

        cluster.addNode(new Node("peer-2", "localhost", LOCAL_PORT + 2));
        cluster.addNode(new Node("peer-3", "localhost", LOCAL_PORT + 3));
        assertThat(dashboard.replicasJson("k")).contains("\"copies\":3").contains("\"quorum\":2");
    }

    @Test
    void bothKeyCountsComeFromOneReading() {
        for (int i = 0; i < 50; i++) {
            store.put("key-" + i, new byte[1]);
        }
        store.delete("key-0");

        String json = dashboard.nodeJson();

        assertThat(json).contains("\"keys\":49,\"keys_raw\":");
        // The counts are kept for a second: a write straight after does not change either of them
        store.put("later", new byte[1]);
        assertThat(dashboard.nodeJson()).contains("\"keys\":49,\"keys_raw\":");
    }
}
