package com.titankv.consistency;

import com.titankv.network.protocol.WriteBatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReplicaWriteBatcherTest {

    private final ExecutorService pool = Executors.newFixedThreadPool(4);
    /** The keys of each request sent, in order. */
    private final List<List<String>> requests = Collections.synchronizedList(new ArrayList<>());
    private final List<String> acknowledged = Collections.synchronizedList(new ArrayList<>());
    private final List<String> failed = Collections.synchronizedList(new ArrayList<>());

    @AfterEach
    void stopPool() {
        pool.shutdownNow();
    }

    private ReplicaWriteBatcher.Write write(String key) {
        return write(key, 10);
    }

    private ReplicaWriteBatcher.Write write(String key, int valueSize) {
        return new ReplicaWriteBatcher.Write(new WriteBatch.Entry(key, new byte[valueSize], 1, 0),
                newer -> acknowledged.add(key + "=" + newer), error -> failed.add(key + ": " + error.getMessage()));
    }

    private static List<String> keys(List<ReplicaWriteBatcher.Write> writes) {
        List<String> keys = new ArrayList<>();
        for (ReplicaWriteBatcher.Write write : writes) {
            keys.add(write.entry.getKey());
        }
        return keys;
    }

    private void awaitAnswered(int count) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (acknowledged.size() + failed.size() < count && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        assertThat(acknowledged.size() + failed.size()).as("writes answered").isEqualTo(count);
    }

    @Test
    void aWriteThatFindsASenderFreeGoesOutAlone() throws Exception {
        ReplicaWriteBatcher batcher = new ReplicaWriteBatcher(pool, writes -> {
            requests.add(keys(writes));
            return new long[writes.size()];
        }, 2, 100, 50, 1 << 20);

        batcher.submit(write("a"));
        awaitAnswered(1);
        batcher.submit(write("b"));
        awaitAnswered(2);

        assertThat(requests).containsExactly(List.of("a"), List.of("b"));
        assertThat(acknowledged).containsExactly("a=0", "b=0");
        assertThat(batcher.getRequestCount()).isEqualTo(2);
        assertThat(batcher.getWriteCount()).isEqualTo(2);
    }

    @Test
    void writesThatArriveWhileTheSenderIsBusyGoOutTogether() throws Exception {
        CountDownLatch firstSent = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ReplicaWriteBatcher batcher = new ReplicaWriteBatcher(pool, writes -> {
            requests.add(keys(writes));
            if (requests.size() == 1) {
                firstSent.countDown();
                await(release); // the replica is waiting for its disk
            }
            return new long[writes.size()];
        }, 1, 100, 50, 1 << 20);

        batcher.submit(write("first"));
        assertThat(firstSent.await(5, TimeUnit.SECONDS)).isTrue();
        for (int i = 0; i < 20; i++) {
            batcher.submit(write("queued-" + i));
        }
        release.countDown();
        awaitAnswered(21);

        assertThat(requests).hasSize(2);
        assertThat(requests.get(0)).containsExactly("first");
        assertThat(requests.get(1)).hasSize(20).startsWith("queued-0", "queued-1").endsWith("queued-19");
        assertThat(failed).isEmpty();
        assertThat(batcher.getRequestCount()).isEqualTo(2);
        assertThat(batcher.getWriteCount()).isEqualTo(21);
    }

    @Test
    void aRequestIsLimitedInWritesAndInBytes() throws Exception {
        CountDownLatch firstSent = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ReplicaWriteBatcher batcher = new ReplicaWriteBatcher(pool, writes -> {
            requests.add(keys(writes));
            if (requests.size() == 1) {
                firstSent.countDown();
                await(release);
            }
            return new long[writes.size()];
        }, 1, 100, 4, 1_000);

        batcher.submit(write("first"));
        assertThat(firstSent.await(5, TimeUnit.SECONDS)).isTrue();
        for (int i = 0; i < 6; i++) {
            batcher.submit(write("small-" + i));
        }
        batcher.submit(write("big-0", 700));
        batcher.submit(write("big-1", 700)); // does not fit in 1,000 bytes with big-0
        batcher.submit(write("huge", 5_000)); // over the limit on its own: still sent, alone
        release.countDown();
        awaitAnswered(10);

        assertThat(requests).containsExactly(List.of("first"),
                List.of("small-0", "small-1", "small-2", "small-3"),
                List.of("small-4", "small-5", "big-0"),
                List.of("big-1"),
                List.of("huge"));
    }

    @Test
    void aRefusedWriteFailsAloneAndASupersededOneReportsTheNewerVersion() throws Exception {
        CountDownLatch firstSent = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ReplicaWriteBatcher batcher = new ReplicaWriteBatcher(pool, writes -> {
            if (writes.size() == 1) {
                firstSent.countDown();
                await(release);
                return new long[1];
            }
            return new long[] {0, WriteBatch.REFUSED, 777};
        }, 1, 100, 50, 1 << 20);

        batcher.submit(write("first"));
        assertThat(firstSent.await(5, TimeUnit.SECONDS)).isTrue();
        batcher.submit(write("stored"));
        batcher.submit(write("refused"));
        batcher.submit(write("superseded"));
        release.countDown();
        awaitAnswered(4);

        assertThat(acknowledged).containsExactlyInAnyOrder("first=0", "stored=0", "superseded=777");
        assertThat(failed).hasSize(1);
        assertThat(failed.get(0)).startsWith("refused: ");
    }

    @Test
    void aFailedRequestFailsEveryWriteInItAndLaterWritesStillGoOut() throws Exception {
        boolean[] broken = {true};
        ReplicaWriteBatcher batcher = new ReplicaWriteBatcher(pool, writes -> {
            if (broken[0]) {
                throw new IOException("connection reset");
            }
            return new long[writes.size()];
        }, 1, 100, 50, 1 << 20);

        batcher.submit(write("lost"));
        awaitAnswered(1);
        assertThat(failed).containsExactly("lost: connection reset");

        broken[0] = false;
        batcher.submit(write("next"));
        awaitAnswered(2);
        assertThat(acknowledged).containsExactly("next=0");
    }

    @Test
    void writesBeyondTheQueueLimitAreRefused() throws Exception {
        CountDownLatch firstSent = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ReplicaWriteBatcher batcher = new ReplicaWriteBatcher(pool, writes -> {
            firstSent.countDown();
            await(release);
            return new long[writes.size()];
        }, 1, 3, 50, 1 << 20);

        batcher.submit(write("in-flight"));
        assertThat(firstSent.await(5, TimeUnit.SECONDS)).isTrue();
        batcher.submit(write("q1"));
        batcher.submit(write("q2"));
        batcher.submit(write("q3"));
        assertThatThrownBy(() -> batcher.submit(write("q4"))).isInstanceOf(RejectedExecutionException.class);

        release.countDown();
        awaitAnswered(4);
        assertThat(failed).isEmpty();
    }

    @Test
    void aPeerThatDoesNotTakeBatchesGetsOneWritePerRequest() throws Exception {
        CountDownLatch firstSent = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ReplicaWriteBatcher batcher = new ReplicaWriteBatcher(pool, writes -> {
            requests.add(keys(writes));
            if (requests.size() == 1) {
                firstSent.countDown();
                await(release);
            }
            return new long[writes.size()];
        }, 1, 100, 50, 1 << 20);
        batcher.disableBatching();

        batcher.submit(write("a"));
        assertThat(firstSent.await(5, TimeUnit.SECONDS)).isTrue();
        batcher.submit(write("b"));
        batcher.submit(write("c"));
        release.countDown();
        awaitAnswered(3);

        assertThat(requests).containsExactly(List.of("a"), List.of("b"), List.of("c"));
    }

    @Test
    void writesAreFailedNotLostWhenThePoolRefusesTheSender() throws Exception {
        ReplicaWriteBatcher batcher = new ReplicaWriteBatcher(task -> {
            throw new RejectedExecutionException("pool is shut down");
        }, writes -> new long[writes.size()], 1, 100, 50, 1 << 20);

        batcher.submit(write("a"));
        awaitAnswered(1);
        assertThat(failed).containsExactly("a: pool is shut down");
    }

    private static void await(CountDownLatch latch) throws IOException {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new IOException("test latch was never released");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }
}
