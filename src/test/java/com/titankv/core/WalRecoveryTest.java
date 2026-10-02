package com.titankv.core;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Crash recovery through the write-ahead log and snapshots. A "crash" is modelled by opening a
 * second store on the same directory without shutting the first one down.
 */
class WalRecoveryTest {

    @TempDir
    Path dataDir;

    private final List<InMemoryStore> stores = new ArrayList<>();

    @BeforeEach
    void enableWal() {
        System.setProperty("titankv.wal.enabled", "true");
        System.setProperty("titankv.wal.max.mb", "1");
    }

    @AfterEach
    void cleanUp() {
        stores.forEach(InMemoryStore::shutdown);
        System.clearProperty("titankv.wal.enabled");
        System.clearProperty("titankv.wal.max.mb");
    }

    private InMemoryStore open() {
        InMemoryStore store = new InMemoryStore(60_000, 512L * 1024 * 1024, dataDir);
        stores.add(store);
        return store;
    }

    @Test
    void everyAcknowledgedWriteSurvivesACrashAcrossSnapshots() {
        InMemoryStore store = open();
        byte[] value = new byte[100_000];
        for (int i = 0; i < 50; i++) {
            store.put("key-" + i, value); // ~5MB total, so several snapshots are taken
        }

        InMemoryStore recovered = open();
        for (int i = 0; i < 50; i++) {
            assertThat(recovered.get("key-" + i)).as("key-" + i).isPresent();
        }
    }

    @Test
    void concurrentWritesDuringSnapshotsRecoverToTheSameState() throws Exception {
        InMemoryStore store = open();
        ExecutorService pool = Executors.newFixedThreadPool(8);
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < 8; t++) {
            int thread = t;
            futures.add(pool.submit(() -> {
                Random random = new Random(thread);
                for (int i = 0; i < 400; i++) {
                    String key = "k" + random.nextInt(200);
                    byte[] value = new byte[2_000 + random.nextInt(2_000)];
                    random.nextBytes(value);
                    if (random.nextInt(10) == 0) {
                        store.delete(key);
                    } else {
                        store.put(key, value);
                    }
                }
            }));
        }
        for (Future<?> future : futures) {
            future.get(60, TimeUnit.SECONDS);
        }
        pool.shutdown();

        InMemoryStore recovered = open();
        assertThat(recovered.keys()).isEqualTo(store.keys());
        for (String key : store.keys()) {
            assertThat(recovered.get(key).orElseThrow().getValueUnsafe())
                    .as(key).isEqualTo(store.get(key).orElseThrow().getValueUnsafe());
        }
    }

    @Test
    void tombstonesAndVersionsSurviveRecoveryButExpiredEntriesDoNot() throws Exception {
        InMemoryStore store = open();
        store.putIfNewer("versioned", "v2".getBytes(StandardCharsets.UTF_8), 2_000, 0);
        store.putIfNewer("deleted", "old".getBytes(StandardCharsets.UTF_8), 1_000, 0);
        store.putIfNewer("deleted", null, 1_001, 0);
        store.put("short-lived", "x".getBytes(StandardCharsets.UTF_8), 50);
        Thread.sleep(100);

        InMemoryStore recovered = open();
        assertThat(recovered.get("versioned").orElseThrow().getTimestamp()).isEqualTo(2_000);
        assertThat(recovered.putIfNewer("versioned", "v1".getBytes(StandardCharsets.UTF_8), 1_500, 0)).isFalse();
        assertThat(recovered.get("deleted")).isEmpty();
        assertThat(recovered.getRaw("deleted").orElseThrow().isTombstone()).isTrue();
        assertThat(recovered.get("short-lived")).isEmpty();
    }

    @Test
    void tornWriteAtTheEndOfTheWalIsIgnored() throws IOException {
        InMemoryStore store = open();
        store.put("first", "1".getBytes(StandardCharsets.UTF_8));
        store.put("second", "2".getBytes(StandardCharsets.UTF_8));

        Path wal = dataDir.resolve("wal.log");
        try (FileChannel channel = FileChannel.open(wal, StandardOpenOption.WRITE)) {
            channel.truncate(channel.size() - 3); // crash halfway through the last append
        }

        InMemoryStore recovered = open();
        assertThat(recovered.get("first")).isPresent();
        assertThat(recovered.get("second")).isEmpty();
    }

    @Test
    void writesBeyondTheMemoryLimitAreRejectedNotEvicted() {
        InMemoryStore store = new InMemoryStore(60_000, 10_000, dataDir);
        stores.add(store);
        store.put("keep", new byte[6_000]);

        assertThatThrownBy(() -> store.put("too-much", new byte[6_000]))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("memory limit");
        assertThat(store.get("keep")).isPresent();
    }
}
