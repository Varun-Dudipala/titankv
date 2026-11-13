package com.titankv.cluster;

import com.titankv.TitanKVClient;
import com.titankv.client.ClientConfig;
import com.titankv.core.KeyValuePair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link ChaosTest} with a network: while writers overwrite keys with increasing versions at
 * QUORUM on a 5-node production-mode cluster, a nemesis keeps breaking it in a random way: one
 * node cut off, the cluster split 2 | 3, a link cut in one direction, or a node crashed and
 * restarted, alone, during a partition, or after being cut off itself. Cut links either refuse requests at once or swallow
 * them until a timeout. The clients can reach every node throughout.
 *
 * Checked, as in ChaosTest: a read never returns a version older than the last one acknowledged
 * for that key, and at the end every key holds a version between its last acknowledged and last
 * attempted write. Also checked here: once the network is whole again, all three replicas of
 * every key converge on the same version.
 */
@Tag("integration")
class PartitionChaosTest {

    private static final int NODES = 5;
    private static final int WRITERS = 8;
    private static final int KEYS_PER_WRITER = 25;
    private static final long CHAOS_MILLIS = 40_000;

    private TestCluster cluster;

    @BeforeEach
    void productionMode() throws Exception {
        System.setProperty("titankv.dev.mode", "false");
        System.setProperty("titankv.cluster.secret", "partition-chaos-secret");
        System.setProperty("titankv.data.dir", Files.createTempDirectory("titankv-partition-chaos").toString());
        System.setProperty("titankv.anti.entropy.interval.ms", "3000");
    }

    @AfterEach
    void stopCluster() {
        if (cluster != null) {
            cluster.close();
        }
        System.clearProperty("titankv.cluster.secret");
        System.clearProperty("titankv.data.dir");
        System.clearProperty("titankv.anti.entropy.interval.ms");
        System.setProperty("titankv.dev.mode", "true");
    }

    @Test
    void acknowledgedWritesSurvivePartitionsAndCrashes() throws Exception {
        cluster = TestCluster.start(24000, NODES);
        runChaos("partition", 7);
    }

    /**
     * The same, with every node's clock wrong by up to 20 seconds and every client's by up to 15,
     * in either direction. Each key has one writer, so its writes must keep their order whatever
     * the clocks say and whichever nodes coordinate them.
     */
    @Test
    void acknowledgedWritesSurvivePartitionsWithSkewedClocks() throws Exception {
        cluster = TestCluster.start(24100, NODES);
        long[] skewMillis = {-20_000, 12_000, 0, 20_000, -7_000};
        for (int i = 0; i < NODES; i++) {
            cluster.node(i).getClusterManager().getClock().setSkewMillis(skewMillis[i]);
        }
        runChaos("skew", 11);
    }

    private void runChaos(String mode, long seed) throws Exception {
        Map<String, Integer> lastAcked = new ConcurrentHashMap<>();
        Map<String, Integer> lastAttempted = new ConcurrentHashMap<>();
        List<String> violations = Collections.synchronizedList(new ArrayList<>());
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicInteger acked = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();
        AtomicInteger readsChecked = new AtomicInteger();

        ExecutorService pool = Executors.newFixedThreadPool(WRITERS + 1);
        List<Future<?>> writers = new ArrayList<>();
        for (int w = 0; w < WRITERS; w++) {
            int writer = w;
            writers.add(pool.submit(() -> {
                ClientConfig config = ClientConfig.builder()
                        .readTimeoutMs(10_000).maxRetries(3).retryDelayMs(50).retryOnFailure(true).build();
                Random random = new Random(writer);
                int version = 0;
                try (TitanKVClient client = new TitanKVClient(config, cluster.addresses())) {
                    if (mode.equals("skew")) {
                        client.setClockSkewMillis((writer - WRITERS / 2) * 3_750L); // -15s .. +11s
                    }
                    while (running.get()) {
                        String key = mode + ":" + writer + ":" + random.nextInt(KEYS_PER_WRITER);
                        version++;
                        lastAttempted.put(key, version);
                        try {
                            client.put(key, Integer.toString(version));
                            lastAcked.put(key, version);
                            acked.incrementAndGet();
                        } catch (Exception e) {
                            failed.incrementAndGet(); // outcome unknown: the write may or may not have landed
                        }
                        String probe = mode + ":" + writer + ":" + random.nextInt(KEYS_PER_WRITER);
                        int expected = lastAcked.getOrDefault(probe, 0);
                        try {
                            int seen = client.getString(probe).map(Integer::parseInt).orElse(0);
                            readsChecked.incrementAndGet();
                            if (seen < expected) {
                                violations.add("stale read of " + probe + ": v" + seen + " after v" + expected
                                        + " was acknowledged");
                            }
                        } catch (Exception e) {
                            // an unavailable read is allowed; a wrong one is not
                        }
                    }
                }
                return null;
            }));
        }

        Future<List<String>> nemesis = pool.submit(() -> {
            Random random = new Random(seed);
            List<String> faults = new ArrayList<>();
            // Every kind of fault happens at least once, in a random order, then they repeat
            List<Integer> kinds = new ArrayList<>(List.of(0, 1, 2, 3, 4, 5));
            Collections.shuffle(kinds, random);
            long end = System.currentTimeMillis() + CHAOS_MILLIS;
            while (System.currentTimeMillis() < end || faults.size() < kinds.size()) {
                cluster.faults().setBlackholeMillis(random.nextBoolean() ? 0 : 400);
                int crashed = -1;
                String fault;
                switch (kinds.get(faults.size() % kinds.size())) {
                    case 0: {
                        int node = random.nextInt(NODES);
                        cluster.isolate(node);
                        fault = "isolate " + (node + 1);
                        break;
                    }
                    case 1: {
                        List<Integer> order = new ArrayList<>(List.of(0, 1, 2, 3, 4));
                        Collections.shuffle(order, random);
                        cluster.partition(new int[] {order.get(0), order.get(1)},
                                new int[] {order.get(2), order.get(3), order.get(4)});
                        fault = "split " + (order.get(0) + 1) + "," + (order.get(1) + 1) + " | rest";
                        break;
                    }
                    case 2: {
                        int from = random.nextInt(NODES);
                        int to = (from + 1 + random.nextInt(NODES - 1)) % NODES;
                        cluster.cutOneWay(from, to);
                        fault = "cut " + (from + 1) + " -> " + (to + 1);
                        break;
                    }
                    case 3: {
                        crashed = random.nextInt(NODES);
                        cluster.crashNode(crashed);
                        fault = "crash " + (crashed + 1);
                        break;
                    }
                    case 4: {
                        // A node crashes while another is cut off, and restarts into the partition
                        int node = random.nextInt(NODES);
                        crashed = (node + 1 + random.nextInt(NODES - 1)) % NODES;
                        cluster.isolate(node);
                        cluster.crashNode(crashed);
                        fault = "isolate " + (node + 1) + " + crash " + (crashed + 1);
                        break;
                    }
                    default: {
                        // A node is cut off, takes writes it cannot replicate, then crashes: the
                        // clients move to other nodes, and its unreplicated writes return with it
                        crashed = random.nextInt(NODES);
                        cluster.isolate(crashed);
                        Thread.sleep(300 + random.nextInt(1_500));
                        cluster.crashNode(crashed);
                        fault = "isolate then crash " + (crashed + 1);
                        break;
                    }
                }
                // Some faults end before the failure detector notices (3s), some last past it
                long hold = random.nextInt(3) == 0 ? 4_000 + random.nextInt(3_000) : 500 + random.nextInt(2_000);
                faults.add(fault + " for " + hold + "ms");
                Thread.sleep(hold);
                if (crashed >= 0) {
                    cluster.startNode(crashed);
                    // a restarted process has a fresh clock object; keep its skew
                    if (mode.equals("skew")) {
                        cluster.node(crashed).getClusterManager().getClock()
                                .setSkewMillis(new long[] {-20_000, 12_000, 0, 20_000, -7_000}[crashed]);
                    }
                    Thread.sleep(500 + random.nextInt(1_000));
                }
                cluster.heal();
                Thread.sleep(1_000 + random.nextInt(2_000));
            }
            return faults;
        });

        List<String> faults = nemesis.get(CHAOS_MILLIS + 120_000, TimeUnit.MILLISECONDS);
        running.set(false);
        for (Future<?> writer : writers) {
            writer.get(60, TimeUnit.SECONDS);
        }
        pool.shutdown();
        cluster.heal();
        cluster.awaitConverged(30_000);

        try (TitanKVClient client = cluster.client()) {
            for (Map.Entry<String, Integer> entry : lastAttempted.entrySet()) {
                String key = entry.getKey();
                int ackedVersion = lastAcked.getOrDefault(key, 0);
                int attemptedVersion = entry.getValue();
                int stored = client.getString(key).map(Integer::parseInt).orElse(0);
                if (stored < ackedVersion || stored > attemptedVersion) {
                    violations.add(key + ": read v" + stored + ", last acked v" + ackedVersion
                            + ", last attempted v" + attemptedVersion);
                }
            }
        }

        // Hints and anti-entropy must leave all three replicas of every key with the same version
        List<String> diverged = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 45_000;
        do {
            diverged.clear();
            for (String key : lastAttempted.keySet()) {
                List<String> copies = new ArrayList<>();
                for (int replica : cluster.replicasOf(key, 0)) {
                    Optional<KeyValuePair> entry = cluster.node(replica).getStore().getRaw(key);
                    copies.add("node " + (replica + 1) + "=" + entry.map(kv -> kv.getTimestamp() + ":"
                            + (kv.isTombstone() ? "tombstone" : new String(kv.getValueUnsafe(), StandardCharsets.UTF_8)))
                            .orElse("none"));
                }
                String first = copies.get(0).substring(copies.get(0).indexOf('='));
                if (!copies.stream().allMatch(copy -> copy.substring(copy.indexOf('=')).equals(first))) {
                    diverged.add(key + " " + copies);
                }
            }
            if (!diverged.isEmpty()) {
                Thread.sleep(500);
            }
        } while (!diverged.isEmpty() && System.currentTimeMillis() < deadline);

        System.out.printf("PartitionChaosTest (%s): %d faults, %d acknowledged writes, %d failed writes, "
                        + "%d reads checked, %d keys checked%n  faults: %s%n", mode, faults.size(), acked.get(),
                failed.get(), readsChecked.get(), lastAttempted.size(), faults);
        assertThat(faults.size()).isGreaterThanOrEqualTo(6);
        assertThat(acked.get()).isGreaterThan(1_000);
        assertThat(violations).as("lost acknowledged writes, stale reads or unwritten values").isEmpty();
        assertThat(diverged).as("replicas that still differ 45s after the network healed").isEmpty();
    }
}
