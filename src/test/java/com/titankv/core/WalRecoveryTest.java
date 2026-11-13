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

    /**
     * A value that expires must come back from the WAL, and from a snapshot, as a tombstone with
     * its version, so the older version it replaced cannot return after a restart.
     */
    @Test
    void expiredValuesAreRecoveredAsTombstones() throws Exception {
        InMemoryStore store = open();
        byte[] old = "old".getBytes(StandardCharsets.UTF_8);
        store.putIfNewer("in-wal", old, 1_000, 0);
        store.putIfNewer("in-wal", "short-lived".getBytes(StandardCharsets.UTF_8), 2_000, System.currentTimeMillis() + 50);
        store.putIfNewer("in-snapshot", old, 1_000, 0);
        store.putIfNewer("in-snapshot", "short-lived".getBytes(StandardCharsets.UTF_8), 2_000,
                System.currentTimeMillis() + 50);
        Thread.sleep(100);

        InMemoryStore fromWal = open();
        assertThat(fromWal.getRaw("in-wal").orElseThrow().isTombstone()).isTrue();
        assertThat(fromWal.putIfNewer("in-wal", old, 1_000, 0)).isFalse();

        byte[] filler = new byte[200_000];
        for (int i = 0; i < 8; i++) {
            store.put("filler-" + i, filler); // 1.6MB: past the 1MB WAL limit, so a snapshot is taken
        }
        assertThat(dataDir.resolve("snapshot.dat")).exists();

        InMemoryStore fromSnapshot = open();
        KeyValuePair recovered = fromSnapshot.getRaw("in-snapshot").orElseThrow();
        assertThat(recovered.isTombstone()).isTrue();
        assertThat(recovered.getTimestamp()).isEqualTo(2_000);
        assertThat(fromSnapshot.putIfNewer("in-snapshot", old, 1_000, 0)).isFalse();
        assertThat(fromSnapshot.get("in-snapshot")).isEmpty();
    }

    /**
     * Recovery must cut the torn record off the log. Otherwise the writes after the first restart
     * are appended behind it, and the next replay stops at the torn record and never reaches them.
     */
    @Test
    void writesAfterRecoveringFromATornTailSurviveTheNextRestart() throws IOException {
        InMemoryStore store = open();
        store.put("first", "1".getBytes(StandardCharsets.UTF_8));
        store.put("torn", "2".getBytes(StandardCharsets.UTF_8));

        Path wal = dataDir.resolve("wal.log");
        long tornSize;
        try (FileChannel channel = FileChannel.open(wal, StandardOpenOption.WRITE)) {
            channel.truncate(channel.size() - 3); // crash halfway through the last append
            tornSize = channel.size();
        }

        InMemoryStore restarted = open();
        assertThat(java.nio.file.Files.size(wal)).as("WAL size after discarding the torn record").isLessThan(tornSize);
        restarted.put("after-crash", "3".getBytes(StandardCharsets.UTF_8));
        restarted.putIfNewer("after-crash-deleted", null, 5_000, 0);

        InMemoryStore restartedAgain = open();
        assertThat(restartedAgain.get("first")).isPresent();
        assertThat(restartedAgain.get("torn")).isEmpty();
        assertThat(restartedAgain.get("after-crash")).isPresent();
        assertThat(restartedAgain.getRaw("after-crash-deleted").orElseThrow().isTombstone()).isTrue();
    }

    /**
     * A bad record with intact records after it is not a torn final write: the log is damaged in
     * the middle. Replaying up to it would drop every later write, and cutting the log there
     * would delete them from disk. The node must refuse to start and leave the file alone.
     */
    @Test
    void corruptionInTheMiddleOfTheWalStopsTheNodeAndKeepsTheFile() throws IOException {
        InMemoryStore store = open();
        byte[] value = new byte[20];
        for (int i = 0; i < 1_000; i++) {
            store.put(String.format("key-%04d", i), value);
        }
        store.shutdown();
        Path wal = dataDir.resolve("wal.log");
        long size = java.nio.file.Files.size(wal);
        long recordSize = size / 1_000;
        flipByteAt(wal, 9 * recordSize + recordSize / 2); // inside the 10th record

        assertThatThrownBy(this::open).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("corrupt at byte " + 9 * recordSize)
                .hasMessageContaining("Refusing to start");
        assertThat(java.nio.file.Files.size(wal)).as("the damaged log is left as it was").isEqualTo(size);

        flipByteAt(wal, 9 * recordSize + recordSize / 2); // repaired, or restored from a copy
        assertThat(open().size()).isEqualTo(1_000);
    }

    @Test
    void garbageAfterTheLastRecordIsCutOffLikeATornWrite() throws IOException {
        InMemoryStore store = open();
        store.put("first", "1".getBytes(StandardCharsets.UTF_8));
        store.shutdown();
        Path wal = dataDir.resolve("wal.log");
        long intact = java.nio.file.Files.size(wal);
        byte[] garbage = new byte[300];
        new Random(7).nextBytes(garbage);
        java.nio.file.Files.write(wal, garbage, StandardOpenOption.APPEND);

        InMemoryStore recovered = open();
        assertThat(recovered.get("first")).isPresent();
        assertThat(java.nio.file.Files.size(wal)).isEqualTo(intact);
    }

    /**
     * A snapshot is written whole and renamed into place, so damage in it is never a torn write.
     */
    @Test
    void corruptSnapshotStopsTheNode() throws IOException {
        InMemoryStore store = open();
        byte[] filler = new byte[200_000];
        for (int i = 0; i < 8; i++) {
            store.put("filler-" + i, filler); // past the 1MB WAL limit, so a snapshot is taken
        }
        store.shutdown();
        Path snapshot = dataDir.resolve("snapshot.dat");
        assertThat(snapshot).exists();
        flipByteAt(snapshot, 100);

        assertThatThrownBy(this::open).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("snapshot.dat").hasMessageContaining("corrupt");
    }

    /**
     * After a WAL write fails the log may end in half a record, and nothing appended behind it
     * could be replayed. The store must stop taking writes rather than acknowledge more of them.
     */
    @Test
    void walFailureStopsFurtherWritesButNotReads() throws IOException {
        InMemoryStore store = open();
        store.put("before", "1".getBytes(StandardCharsets.UTF_8));
        assertThat(store.isHealthy()).isTrue();

        store.closeWalChannel(); // the next append fails, as on a full or failing disk
        assertThatThrownBy(() -> store.put("failed", "2".getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("WAL write failed");
        assertThat(store.isHealthy()).isFalse();
        assertThatThrownBy(() -> store.putIfNewer("later", "3".getBytes(StandardCharsets.UTF_8), 9_000, 0))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("read-only after a WAL failure");
        assertThatThrownBy(() -> store.delete("before")).isInstanceOf(IllegalStateException.class);
        assertThat(store.get("before")).isPresent();
        assertThat(store.get("failed")).isEmpty();

        InMemoryStore restarted = open();
        assertThat(restarted.isHealthy()).isTrue();
        assertThat(restarted.get("before")).isPresent();
        assertThat(restarted.get("failed")).isEmpty();
        assertThat(restarted.get("later")).isEmpty();
    }

    private static void flipByteAt(Path file, long offset) throws IOException {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            java.nio.ByteBuffer one = java.nio.ByteBuffer.allocate(1);
            channel.read(one, offset);
            one.flip();
            one.put(0, (byte) (one.get(0) ^ 0x40));
            channel.write(one, offset);
        }
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
