package com.titankv.cluster;

import com.titankv.TitanKVClient;
import com.titankv.TitanKVServer;
import com.titankv.client.ClientConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;

/**
 * Starts an in-JVM cluster on consecutive ports and waits until every node sees every other node alive.
 */
public final class TestCluster implements AutoCloseable {

    private final int basePort;
    private final List<TitanKVServer> servers = new ArrayList<>();

    private TestCluster(int basePort) {
        this.basePort = basePort;
    }

    public static TestCluster start(int basePort, int size) throws Exception {
        TestCluster cluster = new TestCluster(basePort);
        for (int i = 0; i < size; i++) {
            cluster.servers.add(null);
        }
        for (int i = 0; i < size; i++) {
            cluster.startNode(i);
        }
        cluster.awaitConverged(20_000);
        return cluster;
    }

    /**
     * Start (or restart) a node. Like a real deployment, every node lists the others as seeds, so a
     * restarted node rejoins the cluster instead of starting a cluster of its own.
     */
    public TitanKVServer startNode(int index) throws Exception {
        List<String> others = new ArrayList<>();
        for (int i = 0; i < servers.size(); i++) {
            if (i != index) {
                others.add("localhost:" + (basePort + i));
            }
        }
        String seeds = others.isEmpty() ? null : String.join(",", others);
        TitanKVServer server = new TitanKVServer(basePort + index, "node-" + (index + 1), seeds);
        server.start();
        servers.set(index, server);
        return server;
    }

    /**
     * Start one more node that joins the running cluster.
     */
    public TitanKVServer addNode() throws Exception {
        servers.add(null);
        return startNode(servers.size() - 1);
    }

    public void stopNode(int index) {
        servers.get(index).stop();
    }

    /**
     * Stop a node without the LEAVE broadcast a graceful shutdown sends, so the other nodes
     * still consider it a member and have to detect the failure from missing heartbeats.
     */
    public void crashNode(int index) {
        ClusterManager crashed = servers.get(index).getClusterManager();
        for (Node peer : crashed.getAllNodes()) {
            if (!peer.equals(crashed.getLocalNode())) {
                crashed.removeNode(peer);
            }
        }
        servers.get(index).stop();
    }

    public TitanKVServer node(int index) {
        return servers.get(index);
    }

    public int size() {
        return servers.size();
    }

    public String address(int index) {
        return "localhost:" + (basePort + index);
    }

    public String[] addresses() {
        String[] hosts = new String[servers.size()];
        for (int i = 0; i < hosts.length; i++) {
            hosts[i] = address(i);
        }
        return hosts;
    }

    public TitanKVClient client() {
        return client(addresses());
    }

    public TitanKVClient client(String... hosts) {
        ClientConfig config = ClientConfig.builder()
                .connectTimeoutMs(3000)
                .readTimeoutMs(10_000)
                .maxRetries(3)
                .retryOnFailure(true)
                .build();
        return new TitanKVClient(config, hosts);
    }

    /**
     * Wait until every node holds a version of the key (tombstones count). A QUORUM write returns
     * before the last replica has applied it, so tests that inspect a replica directly wait first.
     */
    public void awaitReplicated(String key) {
        awaitCondition(() -> servers.stream().allMatch(s -> s.getStore().getRaw(key).isPresent()),
                5_000, key + " to reach every replica");
    }

    public void awaitConverged(long timeoutMs) {
        awaitCondition(() -> servers.stream().allMatch(s ->
                s.getClusterManager().getAliveNodeCount() == servers.size()),
                timeoutMs, "all " + servers.size() + " nodes to see each other alive");
    }

    public static void awaitCondition(BooleanSupplier condition, long timeoutMs, String description) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("Interrupted waiting for " + description, e);
            }
        }
        throw new AssertionError("Timed out after " + timeoutMs + "ms waiting for " + description);
    }

    @Override
    public void close() {
        for (TitanKVServer server : servers) {
            if (server != null) {
                try {
                    server.stop();
                } catch (RuntimeException ignored) {
                    // best-effort teardown
                }
            }
        }
    }
}
