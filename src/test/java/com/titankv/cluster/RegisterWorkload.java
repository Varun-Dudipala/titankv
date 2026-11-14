package com.titankv.cluster;

import com.titankv.TitanKVClient;
import com.titankv.client.ClientConfig;
import com.titankv.cluster.HistoryChecker.Kind;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Several clients reading, writing and deleting the same few keys as fast as they can, each
 * operation recorded in a {@link HistoryChecker}. Unlike a workload with one writer per key, the
 * order of writes here depends on the cluster and not on any one client's clock, so it can show
 * a write from one client being lost to, or resurrected over, a write from another.
 *
 * Of the operations 45% are reads, 10% deletes and 45% writes; one write in nine carries a TTL
 * (ten minutes, so nothing expires during a test, but the expiry is stored and replicated).
 */
final class RegisterWorkload {

    private final String name;
    private final String[] addresses;
    private final int clients;
    private final int keys;
    private final long[] clientSkewMillis;
    private final HistoryChecker history = new HistoryChecker();
    private final AtomicBoolean running = new AtomicBoolean();
    private final List<Future<?>> workers = new ArrayList<>();
    private ExecutorService pool;

    /**
     * @param clientSkewMillis how far each client's clock is off, or null for correct clocks
     */
    RegisterWorkload(String name, String[] addresses, int clients, int keys, long[] clientSkewMillis) {
        this.name = name;
        this.addresses = addresses;
        this.clients = clients;
        this.keys = keys;
        this.clientSkewMillis = clientSkewMillis;
    }

    HistoryChecker history() {
        return history;
    }

    List<String> keys() {
        List<String> all = new ArrayList<>();
        for (int i = 0; i < keys; i++) {
            all.add(name + ":" + i);
        }
        return all;
    }

    void start() {
        running.set(true);
        pool = Executors.newFixedThreadPool(clients);
        for (int c = 0; c < clients; c++) {
            int client = c;
            workers.add(pool.submit(() -> {
                run(client);
                return null;
            }));
        }
    }

    void stop() throws Exception {
        running.set(false);
        for (Future<?> worker : workers) {
            worker.get(60, TimeUnit.SECONDS);
        }
        pool.shutdown();
    }

    private void run(int client) {
        ClientConfig config = ClientConfig.builder()
                .readTimeoutMs(10_000).maxRetries(3).retryDelayMs(50).retryOnFailure(true).build();
        Random random = new Random(client * 31L + 7);
        long sequence = 0;
        try (TitanKVClient titan = new TitanKVClient(config, addresses)) {
            if (clientSkewMillis != null) {
                titan.setClockSkewMillis(clientSkewMillis[client % clientSkewMillis.length]);
            }
            while (running.get()) {
                String key = name + ":" + random.nextInt(keys);
                int dice = random.nextInt(100);
                long start = System.nanoTime();
                if (dice < 45) {
                    try {
                        String value = titan.getString(key).orElse(null);
                        history.record(Kind.READ, key, value, start, System.nanoTime(), true, client);
                    } catch (Exception e) {
                        // an unavailable read tells nothing; a wrong one is what the checker looks for
                    }
                } else if (dice < 55) {
                    boolean ok = true;
                    try {
                        titan.delete(key);
                    } catch (Exception e) {
                        ok = false; // outcome unknown: the delete may or may not have happened
                    }
                    history.record(Kind.DELETE, key, null, start, System.nanoTime(), ok, client);
                } else {
                    String value = "c" + client + "-" + (++sequence);
                    boolean ok = true;
                    try {
                        titan.put(key, value.getBytes(StandardCharsets.UTF_8), dice >= 95 ? 600_000 : 0);
                    } catch (Exception e) {
                        ok = false;
                    }
                    history.record(Kind.PUT, key, value, start, System.nanoTime(), ok, client);
                }
            }
        }
    }

    /**
     * Read every key once more, recorded like any other read, so the state the cluster ends in is
     * checked against the history too.
     */
    void readFinalState(TitanKVClient client) throws Exception {
        for (String key : keys()) {
            long start = System.nanoTime();
            String value = client.getString(key).orElse(null);
            history.record(Kind.READ, key, value, start, System.nanoTime(), true, -1);
        }
    }
}
