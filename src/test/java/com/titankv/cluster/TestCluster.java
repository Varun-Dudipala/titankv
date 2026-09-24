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
            cluster.startNode(i);
        }
        cluster.awaitConverged(20_000);
        return cluster;
    }

    public TitanKVServer startNode(int index) throws Exception {
        String seeds = index == 0 ? null : "localhost:" + basePort;
        TitanKVServer server = new TitanKVServer(basePort + index, "node-" + (index + 1), seeds);
        server.start();
        servers.set(index, server);
        return server;
    }

    public void stopNode(int index) {
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
