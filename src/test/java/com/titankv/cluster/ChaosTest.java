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
 * nodes crash and restart one at a time. Afterwards every key must hold a version at least as new
 * as the last acknowledged write to it, and no newer than the last attempted one: acknowledged
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
        Map<String, Integer> lastAcked = new ConcurrentHashMap<>();
        Map<String, Integer> lastAttempted = new ConcurrentHashMap<>();
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicInteger acked = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();

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
                        String key = "chaos:" + writer + ":" + random.nextInt(KEYS_PER_WRITER);
                        version++;
                        lastAttempted.put(key, version);
                        try {
                            client.put(key, Integer.toString(version));
                            lastAcked.put(key, version);
                            acked.incrementAndGet();
                        } catch (Exception e) {
                            failed.incrementAndGet(); // outcome unknown: the write may or may not have landed
                        }
                    }
                }
                return null;
            }));
        }

        Future<Integer> chaos = pool.submit(() -> {
            Random random = new Random(42);
            int crashes = 0;
            long end = System.currentTimeMillis() + CHAOS_MILLIS;
            while (System.currentTimeMillis() < end) {
                int victim = random.nextInt(NODES);
                cluster.crashNode(victim);
                crashes++;
                Thread.sleep(1_500 + random.nextInt(1_500));
                cluster.startNode(victim);
                Thread.sleep(1_500 + random.nextInt(1_500));
            }
            return crashes;
        });

        int crashes = chaos.get(CHAOS_MILLIS + 60_000, TimeUnit.MILLISECONDS);
        running.set(false);
        for (Future<?> writer : writers) {
            writer.get(60, TimeUnit.SECONDS);
        }
        pool.shutdown();
        cluster.awaitConverged(30_000);

        List<String> violations = new ArrayList<>();
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

        System.out.printf("ChaosTest: %d node crashes, %d acknowledged writes, %d failed writes, %d keys checked%n",
                crashes, acked.get(), failed.get(), lastAttempted.size());
        assertThat(crashes).isGreaterThanOrEqualTo(4);
        assertThat(acked.get()).isGreaterThan(1_000);
        assertThat(violations).as("keys that lost an acknowledged write or show an unwritten value").isEmpty();
    }
}
