package com.titankv.network;

import com.titankv.TitanKVClient;
import com.titankv.TitanKVServer;
import com.titankv.client.ClientConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A server with more requests than its workers and their queue can hold says "busy" and stays
 * responsive. Before, a request that did not fit was run by the thread that submitted it, which
 * for a new request is the selector: one fsync on that thread, and no connection is read from,
 * written to or accepted until it returns.
 */
class BusyServerTest {

    private static final int PORT = 19050;
    private static final int CLIENTS = 16;

    private TitanKVServer server;

    @BeforeEach
    void startServer() throws Exception {
        // One worker, one waiting request, and writes that each wait for an fsync
        System.setProperty("titankv.worker.threads", "1");
        System.setProperty("titankv.worker.queue", "1");
        System.setProperty("titankv.wal.enabled", "true");
        System.setProperty("titankv.data.dir", Files.createTempDirectory("titankv-busy").toString());
        server = new TitanKVServer(PORT);
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.stop();
        System.clearProperty("titankv.worker.threads");
        System.clearProperty("titankv.worker.queue");
        System.clearProperty("titankv.wal.enabled");
        System.clearProperty("titankv.data.dir");
    }

    @Test
    void requestsBeyondTheQueueAreAnsweredBusy() throws Exception {
        AtomicInteger written = new AtomicInteger();
        AtomicInteger busy = new AtomicInteger();
        List<String> otherFailures = new ArrayList<>();
        long end = System.currentTimeMillis() + 2_000;

        ExecutorService pool = Executors.newFixedThreadPool(CLIENTS);
        List<Future<?>> clients = new ArrayList<>();
        for (int c = 0; c < CLIENTS; c++) {
            int id = c;
            clients.add(pool.submit(() -> {
                ClientConfig config = ClientConfig.builder().readTimeoutMs(5_000).retryOnFailure(false).build();
                try (TitanKVClient client = new TitanKVClient(config, "localhost:" + PORT)) {
                    for (int i = 0; System.currentTimeMillis() < end; i++) {
                        try {
                            client.put("busy:" + id + ":" + i, "value");
                            written.incrementAndGet();
                        } catch (IOException e) {
                            if (String.valueOf(e.getMessage()).contains("Server busy")) {
                                busy.incrementAndGet();
                            } else {
                                synchronized (otherFailures) {
                                    otherFailures.add(e.toString());
                                }
                            }
                        }
                    }
                }
                return null;
            }));
        }
        for (Future<?> client : clients) {
            client.get(30, TimeUnit.SECONDS); // every request was answered: nobody is left waiting
        }
        pool.shutdown();

        System.out.printf("BusyServerTest: %d written, %d answered busy%n", written.get(), busy.get());
        assertThat(otherFailures).as("failures other than 'busy'").isEmpty();
        assertThat(busy.get()).as("requests answered busy").isPositive();
        assertThat(written.get()).as("requests carried out meanwhile").isPositive();

        // Every write that was acknowledged is there, and the server works as before
        assertThat(server.getStore().size()).isGreaterThanOrEqualTo(written.get());
        try (TitanKVClient client = new TitanKVClient("localhost:" + PORT)) {
            client.put("after", "the rush");
            assertThat(client.getString("after")).contains("the rush");
        }
    }
}
