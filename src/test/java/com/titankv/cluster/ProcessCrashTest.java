package com.titankv.cluster;

import com.titankv.TitanKVClient;
import com.titankv.client.ClientConfig;
import com.titankv.cluster.HistoryChecker.Kind;
import com.titankv.consistency.ConsistencyLevel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Three nodes as separate operating-system processes, killed with SIGKILL under load.
 *
 * The other chaos tests run every node inside the test's JVM, where a "crash" is an orderly stop:
 * threads end, files are closed. Here a node dies between any two instructions, with requests
 * half handled, the WAL and hint files mid-append, and its sockets cut by the kernel; and it comes
 * back as a new process that has only what it wrote to disk. Clients read, write and delete shared
 * keys throughout, and the recorded history is checked as in {@link MultiWriterChaosTest}.
 *
 * What this still cannot show: SIGKILL does not lose data the kernel has accepted but not yet
 * written to disk, so a missing fsync would survive it. That needs a power cut or a fault-injecting
 * filesystem.
 */
@Tag("integration")
class ProcessCrashTest {

    private static final int BASE_PORT = 24600;
    private static final int NODES = 3;
    private static final int CLIENTS = 6;
    private static final int KEYS = 12;
    private static final long CHAOS_MILLIS = 30_000;

    private final Process[] processes = new Process[NODES];
    private Path workDir;

    @AfterEach
    void killEverything() throws Exception {
        for (Process process : processes) {
            if (process != null) {
                process.destroyForcibly().waitFor(10, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void sharedKeysKeepTheirHistoryWhileNodeProcessesAreKilled() throws Exception {
        workDir = Files.createTempDirectory("titankv-process-crash");
        for (int i = 0; i < NODES; i++) {
            start(i);
        }
        for (int i = 0; i < NODES; i++) {
            awaitReady(i, 30_000);
        }

        RegisterWorkload workload = new RegisterWorkload("killed", addresses(), CLIENTS, KEYS, null);
        workload.start();
        List<String> kills = new ArrayList<>();
        try {
            Random random = new Random(29);
            long end = System.currentTimeMillis() + CHAOS_MILLIS;
            while (System.currentTimeMillis() < end || kills.size() < 5) {
                int victim = random.nextInt(NODES);
                long pid = processes[victim].pid();
                processes[victim].destroyForcibly().waitFor(10, TimeUnit.SECONDS); // SIGKILL
                long down = 500 + random.nextInt(2_500);
                kills.add("node " + (victim + 1) + " (pid " + pid + ") down " + down + "ms");
                Thread.sleep(down);
                start(victim);
                awaitReady(victim, 30_000);
                Thread.sleep(1_000 + random.nextInt(1_500));
            }
        } finally {
            workload.stop();
        }
        for (int i = 0; i < NODES; i++) {
            awaitReady(i, 30_000);
        }

        // Every node must end with the same value for every key. A read at ONE through a node
        // returns that node's own copy.
        List<String> diverged = awaitNodesAgree(workload.keys(), 60_000);
        try (TitanKVClient client = client(addresses())) {
            workload.readFinalState(client);
        }
        HistoryChecker history = workload.history();
        List<String> violations = history.check();
        System.out.printf("ProcessCrashTest: %d kills; %d writes and %d deletes acknowledged, %d with unknown "
                        + "outcome, %d reads checked%n  kills: %s%n", kills.size(), history.count(Kind.PUT, true),
                history.count(Kind.DELETE, true), history.count(Kind.PUT, false) + history.count(Kind.DELETE, false),
                history.count(Kind.READ, true), kills);

        assertThat(history.count(Kind.PUT, true)).isGreaterThan(500);
        assertThat(violations).as("reads that an acknowledged write had already made impossible").isEmpty();
        assertThat(diverged).as("keys the nodes still disagree on 60s after the last restart").isEmpty();
    }

    private String[] addresses() {
        String[] addresses = new String[NODES];
        for (int i = 0; i < NODES; i++) {
            addresses[i] = "localhost:" + (BASE_PORT + i);
        }
        return addresses;
    }

    private static TitanKVClient client(String... addresses) {
        return new TitanKVClient(ClientConfig.builder().connectTimeoutMs(3_000).readTimeoutMs(10_000)
                .maxRetries(3).retryOnFailure(true).build(), addresses);
    }

    /**
     * Start node {@code index} as a new process, with the test's own classpath and Java.
     */
    private void start(int index) throws Exception {
        List<String> seeds = new ArrayList<>();
        for (int i = 0; i < NODES; i++) {
            if (i != index) {
                seeds.add("localhost:" + (BASE_PORT + i));
            }
        }
        ProcessBuilder builder = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"),
                "com.titankv.TitanKVServer",
                "--port", Integer.toString(BASE_PORT + index),
                "--node-id", "node-" + (index + 1),
                "--seeds", String.join(",", seeds));
        builder.environment().remove("TITANKV_DEV_MODE");
        builder.environment().put("TITANKV_CLUSTER_SECRET", "process-crash-secret");
        builder.environment().put("TITANKV_DATA_DIR", workDir.resolve("data").toString());
        builder.environment().put("TITANKV_ANTI_ENTROPY_INTERVAL_MS", "3000");
        builder.directory(workDir.toFile());
        File output = workDir.resolve("node-" + (index + 1) + ".out").toFile();
        builder.redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.appendTo(output));
        processes[index] = builder.start();
    }

    /**
     * Wait until the node's own readiness endpoint says it may serve.
     */
    private void awaitReady(int index, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (!processes[index].isAlive()) {
                throw new AssertionError("node " + (index + 1) + " exited with " + processes[index].exitValue()
                        + "; see " + workDir.resolve("node-" + (index + 1) + ".out"));
            }
            try {
                HttpURLConnection connection = (HttpURLConnection) URI.create(
                        "http://localhost:" + (BASE_PORT + index + 90) + "/ready").toURL().openConnection();
                connection.setConnectTimeout(500);
                connection.setReadTimeout(500);
                int status = connection.getResponseCode();
                connection.disconnect();
                if (status == 200) {
                    return;
                }
            } catch (java.io.IOException e) {
                // not listening yet
            }
            Thread.sleep(100);
        }
        throw new AssertionError("node " + (index + 1) + " was not ready after " + timeoutMs + "ms; see "
                + workDir.resolve("node-" + (index + 1) + ".out"));
    }

    private List<String> awaitNodesAgree(List<String> keys, long timeoutMs) throws Exception {
        List<TitanKVClient> clients = new ArrayList<>();
        for (String address : addresses()) {
            clients.add(client(address));
        }
        List<String> diverged = new ArrayList<>();
        try {
            long deadline = System.currentTimeMillis() + timeoutMs;
            do {
                diverged.clear();
                for (String key : keys) {
                    List<Optional<String>> copies = new ArrayList<>();
                    for (TitanKVClient client : clients) {
                        copies.add(client.getString(key, ConsistencyLevel.ONE));
                    }
                    if (copies.stream().distinct().count() > 1) {
                        diverged.add(key + " " + copies);
                    }
                }
                if (!diverged.isEmpty()) {
                    Thread.sleep(500);
                }
            } while (!diverged.isEmpty() && System.currentTimeMillis() < deadline);
        } finally {
            clients.forEach(TitanKVClient::close);
        }
        return diverged;
    }
}
