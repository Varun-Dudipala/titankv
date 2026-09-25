package com.titankv.benchmark;

import com.titankv.TitanKVClient;
import com.titankv.client.ClientConfig;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/**
 * Closed-loop load generator: each thread issues one request at a time with its own client.
 *
 * Every thread first writes its whole key space, so reads hit existing keys, then runs an
 * unmeasured warmup (JIT, connection pools) before the measured run. Latency is recorded per
 * operation and reported as percentiles.
 *
 * Reads are also checked for correctness. Each value starts with an 8-byte sequence number that
 * increases with every write the thread makes, and the thread remembers the last sequence it had
 * acknowledged for each key. A read that returns an older sequence is a stale read; with QUORUM or
 * ALL writes and reads there should be none.
 */
public class LoadGenerator {

    private final String[] hosts;
    private final int threads;
    private final int opsPerThread;
    private final int durationSeconds;
    private final int warmupOpsPerThread;
    private final double readRatio;
    private final int valueSize;
    private final int keysPerThread;
    private final boolean retry;

    private final LongAdder reads = new LongAdder();
    private final LongAdder readMisses = new LongAdder();
    private final LongAdder staleReads = new LongAdder();
    private final LongAdder writes = new LongAdder();
    private final LongAdder errors = new LongAdder();

    public LoadGenerator(String[] hosts, int threads, int opsPerThread, int durationSeconds, int warmupOpsPerThread,
            double readRatio, int valueSize, int keysPerThread, boolean retry) {
        if (valueSize < 8) {
            throw new IllegalArgumentException("--value-size must be at least 8 (values carry a sequence number)");
        }
        this.hosts = hosts;
        this.threads = threads;
        this.opsPerThread = opsPerThread;
        this.durationSeconds = durationSeconds;
        this.warmupOpsPerThread = warmupOpsPerThread;
        this.readRatio = readRatio;
        this.valueSize = valueSize;
        this.keysPerThread = keysPerThread;
        this.retry = retry;
    }

    /**
     * @param timeline print throughput and errors every second of the measured run
     */
    public Result run(boolean timeline) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CyclicBarrier start = new CyclicBarrier(threads + 1);
        CyclicBarrier measured = new CyclicBarrier(threads + 1);
        long[][] latencies = new long[threads][];
        Future<?>[] futures = new Future<?>[threads];
        AtomicBoolean stop = new AtomicBoolean();

        for (int t = 0; t < threads; t++) {
            int thread = t;
            futures[t] = pool.submit(() -> {
                Worker worker = new Worker(thread);
                try (TitanKVClient client = createClient()) {
                    for (int k = 0; k < keysPerThread; k++) {
                        worker.write(client, key(thread, k));
                    }
                    start.await();
                    worker.runOps(client, warmupOpsPerThread, null, false);
                    measured.await();
                    latencies[thread] = worker.runMeasured(client, stop);
                }
                return null;
            });
        }

        System.out.println("Preloading " + threads * keysPerThread + " keys...");
        start.await();
        System.out.println("Warming up (" + threads * warmupOpsPerThread + " ops)...");
        measured.await();
        long startNanos = System.nanoTime();

        ScheduledExecutorService reporter = Executors.newSingleThreadScheduledExecutor();
        if (timeline) {
            long[] previous = new long[2];
            reporter.scheduleAtFixedRate(() -> {
                long ops = reads.sum() + writes.sum();
                long failed = errors.sum();
                System.out.printf("TIMELINE,%d,%d,%d%n", (System.nanoTime() - startNanos) / 1_000_000_000L,
                        ops - previous[0], failed - previous[1]);
                previous[0] = ops;
                previous[1] = failed;
            }, 1, 1, TimeUnit.SECONDS);
        }
        if (durationSeconds > 0) {
            reporter.schedule(() -> stop.set(true), durationSeconds, TimeUnit.SECONDS);
        }
        for (Future<?> future : futures) {
            future.get();
        }
        long elapsed = System.nanoTime() - startNanos;
        reporter.shutdownNow();
        pool.shutdown();

        long[] all = Arrays.stream(latencies).flatMapToLong(Arrays::stream).filter(v -> v > 0).sorted().toArray();
        return new Result(reads.sum(), readMisses.sum(), staleReads.sum(), writes.sum(), errors.sum(), elapsed, all);
    }

    /**
     * One client thread's state: its write sequence and the last acknowledged sequence per key.
     */
    private final class Worker {
        private final int thread;
        private final Map<String, Long> acked = new HashMap<>();
        private long sequence;

        Worker(int thread) {
            this.thread = thread;
        }

        void write(TitanKVClient client, String key) throws Exception {
            long seq = ++sequence;
            byte[] value = new byte[valueSize];
            ThreadLocalRandom.current().nextBytes(value);
            ByteBuffer.wrap(value).putLong(seq);
            client.put(key, value);
            acked.put(key, seq);
        }

        long[] runMeasured(TitanKVClient client, AtomicBoolean stop) {
            if (durationSeconds <= 0) {
                long[] samples = new long[opsPerThread];
                runOps(client, opsPerThread, samples, true);
                return samples;
            }
            long[] samples = new long[1 << 16];
            int count = 0;
            while (!stop.get()) {
                if (count == samples.length) {
                    samples = Arrays.copyOf(samples, samples.length * 2);
                }
                long[] one = new long[1];
                runOps(client, 1, one, true);
                samples[count++] = one[0];
            }
            return Arrays.copyOf(samples, count);
        }

        void runOps(TitanKVClient client, int count, long[] samples, boolean measured) {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            for (int i = 0; i < count; i++) {
                String key = key(thread, random.nextInt(keysPerThread));
                boolean read = random.nextDouble() < readRatio;
                long begin = System.nanoTime();
                try {
                    if (read) {
                        Optional<byte[]> value = client.get(key);
                        if (measured) {
                            reads.increment();
                            if (value.isEmpty()) {
                                readMisses.increment();
                            } else if (ByteBuffer.wrap(value.get()).getLong() < acked.getOrDefault(key, 0L)) {
                                staleReads.increment();
                            }
                        }
                    } else {
                        write(client, key);
                        if (measured) {
                            writes.increment();
                        }
                    }
                    if (samples != null) {
                        samples[i] = System.nanoTime() - begin;
                    }
                } catch (Exception e) {
                    if (measured) {
                        errors.increment();
                    }
                }
            }
        }
    }

    private static String key(int thread, int index) {
        return "bench:" + thread + ":" + index;
    }

    private TitanKVClient createClient() {
        return new TitanKVClient(ClientConfig.builder()
                .connectTimeoutMs(2000)
                .readTimeoutMs(10000)
                .maxConnectionsPerHost(2)
                .retryOnFailure(retry)
                .maxRetries(3)
                .retryDelayMs(20)
                .build(), hosts);
    }

    public static final class Result {
        final long reads;
        final long readMisses;
        final long staleReads;
        final long writes;
        final long errors;
        final long elapsedNanos;
        final long[] sortedLatencies;

        Result(long reads, long readMisses, long staleReads, long writes, long errors, long elapsedNanos,
                long[] sortedLatencies) {
            this.reads = reads;
            this.readMisses = readMisses;
            this.staleReads = staleReads;
            this.writes = writes;
            this.errors = errors;
            this.elapsedNanos = elapsedNanos;
            this.sortedLatencies = sortedLatencies;
        }

        double seconds() {
            return elapsedNanos / 1e9;
        }

        double throughput() {
            return (reads + writes) / seconds();
        }

        double percentileMs(double p) {
            if (sortedLatencies.length == 0) {
                return 0;
            }
            int index = (int) Math.min(sortedLatencies.length - 1, Math.ceil(p * sortedLatencies.length) - 1);
            return sortedLatencies[Math.max(0, index)] / 1e6;
        }

        void print() {
            long ops = reads + writes;
            System.out.println();
            System.out.println("Results");
            System.out.println("-------------------------------------------");
            System.out.printf("Operations:        %,d (%,d reads, %,d writes)%n", ops, reads, writes);
            System.out.printf("Errors:            %,d%n", errors);
            System.out.printf("Read misses:       %,d (should be 0: every key is preloaded)%n", readMisses);
            System.out.printf("Stale reads:       %,d (reads older than the thread's last acknowledged write)%n",
                    staleReads);
            System.out.printf("Duration:          %.2f s%n", seconds());
            System.out.printf("Throughput:        %,.0f ops/sec (%,.0f reads/sec, %,.0f writes/sec)%n",
                    ops / seconds(), reads / seconds(), writes / seconds());
            System.out.printf("Latency p50/p95/p99/max: %.3f / %.3f / %.3f / %.3f ms%n",
                    percentileMs(0.50), percentileMs(0.95), percentileMs(0.99), percentileMs(1.0));
            System.out.printf("CSV,%.0f,%.3f,%.3f,%.3f,%.3f,%d,%d,%d,%d%n", throughput(), percentileMs(0.50),
                    percentileMs(0.95), percentileMs(0.99), percentileMs(1.0), errors, readMisses, staleReads, ops);
        }
    }

    public static void main(String[] args) throws Exception {
        String[] hosts = {"localhost:9001"};
        int threads = 16;
        int ops = 20_000;
        int duration = 0;
        int warmup = 2_000;
        double readRatio = 0.8;
        int valueSize = 100;
        int keys = 1_000;
        boolean timeline = false;
        boolean retry = false;

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            switch (arg) {
                case "--help":
                    printUsage();
                    return;
                case "--timeline":
                    timeline = true;
                    continue;
                case "--retry":
                    retry = true;
                    continue;
                default:
                    break;
            }
            if (i + 1 >= args.length) {
                throw new IllegalArgumentException("Missing value for " + arg);
            }
            String value = args[++i];
            switch (arg) {
                case "--hosts": hosts = value.split(","); break;
                case "--threads": threads = Integer.parseInt(value); break;
                case "--ops": ops = Integer.parseInt(value); break;
                case "--duration": duration = Integer.parseInt(value); break;
                case "--warmup": warmup = Integer.parseInt(value); break;
                case "--read-ratio": readRatio = Double.parseDouble(value); break;
                case "--value-size": valueSize = Integer.parseInt(value); break;
                case "--keys": keys = Integer.parseInt(value); break;
                default: throw new IllegalArgumentException("Unknown option " + arg);
            }
        }

        System.out.println("TitanKV Load Generator");
        System.out.println("-------------------------------------------");
        System.out.println("Hosts:            " + String.join(", ", hosts));
        System.out.println("Threads:          " + threads);
        System.out.println(duration > 0
                ? "Duration:         " + duration + "s (after " + warmup + " warmup ops per thread)"
                : "Ops per thread:   " + ops + " (after " + warmup + " warmup)");
        System.out.println("Read ratio:       " + readRatio);
        System.out.println("Value size:       " + valueSize + " bytes");
        System.out.println("Keys per thread:  " + keys);
        System.out.println("Client retries:   " + (retry ? "on (fails over to another node)" : "off"));
        System.out.println("CPUs:             " + Runtime.getRuntime().availableProcessors());
        System.out.println("-------------------------------------------");

        new LoadGenerator(hosts, threads, ops, duration, warmup, readRatio, valueSize, keys, retry)
                .run(timeline).print();
    }

    private static void printUsage() {
        System.out.println("Usage: LoadGenerator [--hosts h1:p1,h2:p2] [--threads 16] [--ops 20000 | --duration SECONDS]");
        System.out.println("                     [--warmup 2000] [--read-ratio 0.8] [--value-size 100] [--keys 1000]");
        System.out.println("                     [--retry] [--timeline]");
        System.out.println("  --retry     let clients fail over to another node on error");
        System.out.println("  --timeline  print TIMELINE,<second>,<ops>,<errors> every second");
    }
}
