package com.titankv.consistency;

import com.titankv.network.protocol.WriteBatch;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

/**
 * Sends one peer its replica writes, several to a request when they queue up.
 *
 * A replica call holds a thread and a connection until the replica answers. When that takes a
 * fraction of a millisecond, a few threads carry all the writes there are, one per request. When
 * the replica waits for its disk before answering, each call takes milliseconds, and with one
 * write per call a peer could never be sent more writes at a time than it has sender threads.
 * Here a sender that becomes free takes everything that queued up meanwhile (up to a limit) and
 * sends it as one request, which the replica makes durable with one fsync. Nothing waits to
 * fill a batch: a write that finds a sender free goes out alone, at once.
 */
final class ReplicaWriteBatcher {

    /** Sends the writes to the peer and returns its answer to each (see {@link WriteBatch}). */
    interface Sender {
        long[] send(List<Write> writes) throws IOException;
    }

    /** A write on its way to the peer, and what to do with the peer's answer. */
    static final class Write {
        final WriteBatch.Entry entry;
        private final int size;
        private final LongConsumer acknowledged;
        private final Consumer<Exception> failed;

        /**
         * @param acknowledged receives 0 if the peer stored this version, else the newer one it kept
         * @param failed       receives why the peer does not have the write
         */
        Write(WriteBatch.Entry entry, LongConsumer acknowledged, Consumer<Exception> failed) {
            this.entry = entry;
            this.size = entry.encodedSize();
            this.acknowledged = acknowledged;
            this.failed = failed;
        }
    }

    private final Executor pool;
    private final Sender sender;
    private final int maxSenders;
    private final int maxQueued;
    private final int maxBatchBytes;
    private final ArrayDeque<Write> queue = new ArrayDeque<>();
    private int senders;
    private volatile int maxBatch;
    private final LongAdder requests = new LongAdder();
    private final LongAdder writes = new LongAdder();

    /**
     * @param pool          runs the blocking sends
     * @param maxSenders    requests in flight to the peer at once
     * @param maxQueued     writes that may wait for a sender; further ones are refused
     * @param maxBatch      writes per request
     * @param maxBatchBytes encoded bytes per request (one write is always sent, whatever its size)
     */
    ReplicaWriteBatcher(Executor pool, Sender sender, int maxSenders, int maxQueued, int maxBatch,
            int maxBatchBytes) {
        this.pool = pool;
        this.sender = sender;
        this.maxSenders = maxSenders;
        this.maxQueued = maxQueued;
        this.maxBatch = maxBatch;
        this.maxBatchBytes = maxBatchBytes;
    }

    /** @return requests sent to the peer so far */
    long getRequestCount() {
        return requests.sum();
    }

    /** @return writes sent to the peer so far, in those requests */
    long getWriteCount() {
        return writes.sum();
    }

    /** One write per request from now on, for a peer that does not understand batches. */
    void disableBatching() {
        maxBatch = 1;
    }

    /**
     * @throws RejectedExecutionException if too many writes are already waiting for this peer,
     *                                    which means its calls are not completing
     */
    void submit(Write write) {
        boolean start = false;
        synchronized (queue) {
            if (queue.size() >= maxQueued) {
                throw new RejectedExecutionException("Too many writes waiting for this replica");
            }
            queue.add(write);
            if (senders < maxSenders) {
                senders++;
                start = true;
            }
        }
        if (start) {
            try {
                pool.execute(this::sendQueued);
            } catch (RejectedExecutionException e) {
                List<Write> abandoned = new ArrayList<>();
                synchronized (queue) {
                    senders--;
                    if (senders == 0) {
                        // Nobody is left to send what is queued
                        abandoned.addAll(queue);
                        queue.clear();
                    }
                }
                for (Write each : abandoned) {
                    each.failed.accept(e);
                }
            }
        }
    }

    private void sendQueued() {
        List<Write> batch = new ArrayList<>();
        while (true) {
            batch.clear();
            synchronized (queue) {
                int bytes = 0;
                Write next;
                while ((next = queue.peek()) != null && batch.size() < maxBatch
                        && (batch.isEmpty() || bytes + next.size <= maxBatchBytes)) {
                    bytes += next.size;
                    batch.add(queue.poll());
                }
                if (batch.isEmpty()) {
                    senders--;
                    return;
                }
            }
            requests.increment();
            writes.add(batch.size());
            try {
                long[] results = sender.send(batch);
                for (int i = 0; i < batch.size(); i++) {
                    Write write = batch.get(i);
                    if (results[i] == WriteBatch.REFUSED) {
                        write.failed.accept(new IOException("The replica could not take the write"));
                    } else {
                        write.acknowledged.accept(results[i]);
                    }
                }
            } catch (IOException | RuntimeException e) {
                for (Write write : batch) {
                    write.failed.accept(e);
                }
            }
        }
    }
}
