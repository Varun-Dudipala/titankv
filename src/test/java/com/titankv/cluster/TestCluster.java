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
    private final FaultInjector faults = new FaultInjector();

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
        server.getClusterManager().setLinkFilter(faults); // before start, so its first gossip is covered
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
     * Stop a node as if it crashed: no LEAVE is sent, so the other nodes have to detect the
     * failure from missing heartbeats.
     */
    public void crashNode(int index) {
        servers.get(index).crash();
    }

    // ==================== Network faults ====================

    /**
     * Cut the network between two groups of nodes (given by index), in both directions. Clients are
     * not affected: a test puts a client on one side by giving it only that side's addresses.
     */
    public void partition(int[] sideA, int[] sideB) {
        faults.partition(ports(sideA), ports(sideB));
    }

    /** Cut one node off from every other node. */
    public void isolate(int index) {
        List<Integer> others = new ArrayList<>();
        for (int i = 0; i < servers.size(); i++) {
            if (i != index) {
                others.add(basePort + i);
            }
        }
        faults.partition(List.of(basePort + index), others);
    }

    /** Cut traffic from one node to another; the opposite direction keeps working. */
    public void cutOneWay(int from, int to) {
        faults.cut(basePort + from, basePort + to);
    }

    /** Add latency to every request one node sends another. */
    public void slowLink(int from, int to, long millis) {
        faults.delay(basePort + from, basePort + to, millis);
    }

    /** Restore every link. */
    public void heal() {
        faults.heal();
    }

    public FaultInjector faults() {
        return faults;
    }

    private List<Integer> ports(int[] indexes) {
        List<Integer> ports = new ArrayList<>();
        for (int index : indexes) {
            ports.add(basePort + index);
        }
        return ports;
    }

    /**
     * The indexes of the nodes that replicate a key, as node {@code viewOf} sees the ring.
     */
    public List<Integer> replicasOf(String key, int viewOf) {
        List<Integer> replicas = new ArrayList<>();
        for (Node replica : servers.get(viewOf).getClusterManager().getReplicasForKey(key, 3)) {
            replicas.add(replica.getPort() - basePort);
        }
        return replicas;
    }

    /**
     * Wait for every replica of every key to hold the same entry: version, value or tombstone, and
     * expiry. Hints and anti-entropy should get them there once the network is whole.
     *
     * @return the keys whose replicas still differ when the time is up, with each copy (empty if none)
     */
    public List<String> awaitReplicasAgree(java.util.Collection<String> keys, long timeoutMs)
            throws InterruptedException {
        List<String> diverged = new ArrayList<>();
        long deadline = System.currentTimeMillis() + timeoutMs;
        do {
            diverged.clear();
            for (String key : keys) {
                List<String> copies = new ArrayList<>();
                boolean same = true;
                for (int replica : replicasOf(key, 0)) {
                    String copy = servers.get(replica).getStore().getRaw(key)
                            .map(kv -> kv.getTimestamp() + ":" + (kv.isTombstone() ? "tombstone"
                                    : new String(kv.getValueUnsafe(), java.nio.charset.StandardCharsets.UTF_8))
                                    + ":" + kv.getExpiresAt())
                            .orElse("none");
                    same &= copies.isEmpty() || copy.equals(copies.get(0).substring(copies.get(0).indexOf('=') + 1));
                    copies.add("node " + (replica + 1) + "=" + copy);
                }
                if (!same) {
                    diverged.add(key + " " + copies);
                }
            }
            if (!diverged.isEmpty()) {
                Thread.sleep(500);
            }
        } while (!diverged.isEmpty() && System.currentTimeMillis() < deadline);
        return diverged;
    }

    /**
     * Wait until node {@code index} considers exactly {@code alive} members (itself included) up.
     */
    public void awaitAliveCount(int index, int alive, long timeoutMs) {
        awaitCondition(() -> servers.get(index).getClusterManager().getAliveNodeCount() == alive,
                timeoutMs, "node " + (index + 1) + " to see " + alive + " nodes alive");
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
                s.getClusterManager().getAliveNodeCount() == servers.size() && s.getClusterManager().isReady()),
                timeoutMs, "all " + servers.size() + " nodes to see each other alive and be ready");
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
