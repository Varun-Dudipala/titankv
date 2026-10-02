package com.titankv.cluster;

import com.titankv.TitanKVClient;
import com.titankv.client.ClientConfig;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Many concurrent clients against a 3-node cluster with QUORUM reads and writes.
 * Before request handling became non-blocking, this load starved every node's worker
 * pool and requests stalled until replica timeouts fired.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ConcurrentLoadTest {

    private static final int BASE_PORT = 19600;
    private static final int THREADS = 32;
    private static final int OPS_PER_THREAD = 300;
    private TestCluster cluster;

    @BeforeAll
    void startCluster() throws Exception {
        cluster = TestCluster.start(BASE_PORT, 3);
    }

    @AfterAll
    void stopCluster() {
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    void mixedLoadCompletesWithoutErrorsAndReadsSeeLatestWrite() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        AtomicInteger errors = new AtomicInteger();
        AtomicInteger staleReads = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();
        long start = System.nanoTime();

        for (int t = 0; t < THREADS; t++) {
            int thread = t;
            futures.add(pool.submit(() -> {
                ClientConfig config = ClientConfig.builder()
                        .readTimeoutMs(10_000)
                        .retryOnFailure(false)
                        .build();
                Map<String, String> latest = new HashMap<>();
                Random random = new Random(thread);
                try (TitanKVClient client = new TitanKVClient(config, cluster.addresses())) {
                    for (int i = 0; i < OPS_PER_THREAD; i++) {
                        String key = "load:" + thread + ":" + random.nextInt(50);
                        try {
                            if (random.nextDouble() < 0.8 && latest.containsKey(key)) {
                                String value = client.getString(key).orElse(null);
                                if (!latest.get(key).equals(value)) {
                                    staleReads.incrementAndGet();
                                }
                            } else {
                                String value = "v" + i;
                                client.put(key, value.getBytes(StandardCharsets.UTF_8));
                                latest.put(key, value);
                            }
                        } catch (Exception e) {
                            errors.incrementAndGet();
                        }
                    }
                }
            }));
        }
        for (Future<?> future : futures) {
            future.get(120, TimeUnit.SECONDS);
        }
        pool.shutdown();
        double seconds = (System.nanoTime() - start) / 1e9;
        System.out.printf("ConcurrentLoadTest: %d ops in %.2fs (%.0f ops/sec)%n",
                THREADS * OPS_PER_THREAD, seconds, THREADS * OPS_PER_THREAD / seconds);

        assertThat(errors.get()).as("failed operations").isZero();
        assertThat(staleReads.get()).as("QUORUM reads that missed the thread's own QUORUM write").isZero();
        assertThat(seconds).as("elapsed seconds").isLessThan(60);
    }
}
