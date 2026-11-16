package com.titankv.core;

import com.titankv.util.HybridLogicalClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Writes that do not hold a thread while they wait for the disk. One thread fsyncs the log and
 * each fsync releases every write logged before it started.
 */
class GroupCommitTest {

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

    private static long version(int n) {
        return HybridLogicalClock.encode(1_700_000_000_000L + n);
    }

    private static void awaitAll(List<CompletableFuture<Long>> writes) throws Exception {
        CompletableFuture.allOf(writes.toArray(new CompletableFuture<?>[0])).get(60, TimeUnit.SECONDS);
    }

    /**
     * One thread hands the store 3,000 writes without waiting for any of them. A store that
     * synced once per write would need 3,000 fsyncs; here writes logged during one fsync are
     * all released by the next.
     */
    @Test
    void writesThatDoNotWaitShareFsyncsAndSurviveACrash() throws Exception {
        InMemoryStore store = open();
        int count = 3_000;
        List<CompletableFuture<Long>> writes = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            writes.add(store.putOrSupersededAsync("key-" + i, ("value-" + i).getBytes(), version(i), 0));
        }
        awaitAll(writes);

        for (CompletableFuture<Long> write : writes) {
            assertThat(write.get()).as("superseded by").isZero();
        }
        System.out.printf("GroupCommitTest: %d writes made durable by %d fsyncs%n", count, store.getWalSyncCount());
        assertThat(store.getWalSyncCount()).as("fsyncs for " + count + " writes").isPositive().isLessThan(count);

        InMemoryStore recovered = open(); // the first store was not shut down: a crash
        for (int i = 0; i < count; i++) {
            assertThat(recovered.get("key-" + i)).as("key-" + i).isPresent();
        }
    }

    @Test
    void waitingWritersAndNonWaitingWritersShareTheSameLog() throws Exception {
        InMemoryStore store = open();
        List<CompletableFuture<Long>> writes = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();
        for (int t = 0; t < 4; t++) {
            int thread = t;
            threads.add(new Thread(() -> {
                for (int i = 0; i < 100; i++) {
                    store.putIfNewer("sync-" + thread + "-" + i, new byte[10], version(i), 0); // waits
                }
            }));
        }
        threads.forEach(Thread::start);
        for (int i = 0; i < 400; i++) {
            writes.add(store.putOrSupersededAsync("async-" + i, new byte[10], version(i), 0));
        }
        for (Thread thread : threads) {
            thread.join(60_000);
            assertThat(thread.isAlive()).isFalse();
        }
        awaitAll(writes);

        InMemoryStore recovered = open();
        assertThat(recovered.rawSize()).isEqualTo(800);
    }

    @Test
    void aWriteBehindANewerVersionReportsThatVersionAtOnce() throws Exception {
        InMemoryStore store = open();
        store.putOrSupersededAsync("key", "new".getBytes(), version(2), 0).get(10, TimeUnit.SECONDS);

        CompletableFuture<Long> older = store.putOrSupersededAsync("key", "old".getBytes(), version(1), 0);

        assertThat(older.get(10, TimeUnit.SECONDS)).isEqualTo(version(2));
        assertThat(store.get("key").orElseThrow().getValue()).isEqualTo("new".getBytes());
        // the same version again is not superseded, and writes nothing
        assertThat(store.putOrSupersededAsync("key", "new".getBytes(), version(2), 0).get(10, TimeUnit.SECONDS))
                .isZero();
    }

    /** Snapshots replace the log while writes are waiting on it; none may hang or be lost. */
    @Test
    void writesWaitingAcrossSnapshotsAreReleasedAndRecovered() throws Exception {
        InMemoryStore store = open();
        List<CompletableFuture<Long>> writes = new ArrayList<>();
        for (int i = 0; i < 60; i++) {
            writes.add(store.putOrSupersededAsync("big-" + i, new byte[100_000], version(i), 0)); // 6 MB, 1 MB log
        }
        awaitAll(writes);

        InMemoryStore recovered = open();
        for (int i = 0; i < 60; i++) {
            assertThat(recovered.get("big-" + i)).as("big-" + i).isPresent();
        }
    }

    @Test
    void clearWithWritesWaitingDoesNotDeadlock() throws Exception {
        InMemoryStore store = open();
        List<CompletableFuture<Long>> writes = new ArrayList<>();
        for (int round = 0; round < 20; round++) {
            for (int i = 0; i < 50; i++) {
                writes.add(store.putOrSupersededAsync("key-" + i, new byte[10], version(round * 100 + i), 0));
            }
            store.clear();
        }
        awaitAll(writes);
        store.putOrSupersededAsync("after", "clear".getBytes(), version(9_999), 0).get(10, TimeUnit.SECONDS);

        InMemoryStore recovered = open();
        assertThat(recovered.rawSize()).isEqualTo(1);
        assertThat(recovered.get("after")).isPresent();
    }

    @Test
    void aFailedLogFailsTheWriteInsteadOfAcknowledgingIt() throws Exception {
        InMemoryStore store = open();
        store.putOrSupersededAsync("before", "ok".getBytes(), version(1), 0).get(10, TimeUnit.SECONDS);
        store.closeWalChannel(); // the disk is gone

        CompletableFuture<Long> write = store.putOrSupersededAsync("after", "lost".getBytes(), version(2), 0);

        assertThatThrownBy(() -> write.get(10, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IllegalStateException.class);
        assertThat(store.isHealthy()).isFalse();
        CompletableFuture<Long> later = store.putOrSupersededAsync("later", "x".getBytes(), version(3), 0);
        assertThat(later).isCompletedExceptionally();
    }

    @Test
    void shutdownAnswersEveryWaitingWrite() throws Exception {
        InMemoryStore store = open();
        List<CompletableFuture<Long>> writes = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            writes.add(store.putOrSupersededAsync("key-" + i, new byte[10], version(i), 0));
        }
        store.shutdown();

        for (CompletableFuture<Long> write : writes) {
            // durable or refused, never left hanging
            try {
                write.get(10, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                assertThat(e.getCause()).isInstanceOf(IllegalStateException.class);
            }
        }
        assertThat(store.putOrSupersededAsync("late", new byte[1], version(1_000), 0)).isCompletedExceptionally();
    }
}
