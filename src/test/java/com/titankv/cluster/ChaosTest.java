package com.titankv.cluster;

import com.titankv.TitanKVClient;
import com.titankv.client.ClientConfig;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Jepsen-style durability check. Writers overwrite keys with increasing versions at QUORUM while
 * nodes crash (or shut down gracefully) and restart one at a time. Reads during the run must never
 * go back past an acknowledged write, and afterwards every key must hold a version at least as new
 * as the last acknowledged write to it and no newer than the last attempted one: acknowledged
 * writes are never lost and values never come from nowhere.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ChaosTest {

    private static final int BASE_PORT = 19950;
    private static final int NODES = 5;
    private static final int WRITERS = 8;
    private static final int KEYS_PER_WRITER = 25;
    private static final long CHAOS_MILLIS = 20_000;

    private TestCluster cluster;

    @BeforeAll
    void startCluster() throws Exception {
        System.setProperty("titankv.dev.mode", "false");
        System.setProperty("titankv.cluster.secret", "chaos-secret");
        System.setProperty("titankv.data.dir", Files.createTempDirectory("titankv-chaos").toString());
        cluster = TestCluster.start(BASE_PORT, NODES);
    }

    @AfterAll
    void stopCluster() {
        if (cluster != null) {
            cluster.close();
        }
        System.clearProperty("titankv.cluster.secret");
        System.clearProperty("titankv.data.dir");
        System.setProperty("titankv.dev.mode", "true");
    }

    @Test
    void acknowledgedWritesSurviveRepeatedNodeCrashes() throws Exception {
        runChaos("crash", false);
    }

    /**
     * A rolling restart: nodes shut down gracefully (announcing it) and come back. A shut-down node
     * must stay a replica of its keys, or its keys would move to nodes that do not have them.
     */
    @Test
    void acknowledgedWritesSurviveRollingRestarts() throws Exception {
        runChaos("restart", true);
    }

    /**
     * Each key has a single writer, so besides the final check, a writer reading one of its own keys
     * must see at least the version it last had acknowledged (QUORUM reads overlap QUORUM writes).
     */
    private void runChaos(String mode, boolean graceful) throws Exception {
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

        Future<Integer> chaos = pool.submit(() -> {
            Random random = new Random(42);
            int restarts = 0;
            long end = System.currentTimeMillis() + CHAOS_MILLIS;
            while (System.currentTimeMillis() < end) {
                int victim = random.nextInt(NODES);
                if (graceful) {
                    cluster.stopNode(victim);
                } else {
                    cluster.crashNode(victim);
                }
                restarts++;
                Thread.sleep(1_500 + random.nextInt(1_500));
                cluster.startNode(victim);
                Thread.sleep(1_500 + random.nextInt(1_500));
            }
            return restarts;
        });

        int restarts = chaos.get(CHAOS_MILLIS + 60_000, TimeUnit.MILLISECONDS);
        running.set(false);
        for (Future<?> writer : writers) {
            writer.get(60, TimeUnit.SECONDS);
        }
        pool.shutdown();
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

        System.out.printf("ChaosTest (%s): %d node %s, %d acknowledged writes, %d failed writes, %d reads checked, "
                        + "%d keys checked%n", mode, restarts, graceful ? "restarts" : "crashes", acked.get(),
                failed.get(), readsChecked.get(), lastAttempted.size());
        assertThat(restarts).isGreaterThanOrEqualTo(4);
        assertThat(acked.get()).isGreaterThan(1_000);
        assertThat(violations).as("lost acknowledged writes, stale reads or unwritten values").isEmpty();
    }
}
