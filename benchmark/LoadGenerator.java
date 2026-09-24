package com.titankv.benchmark;

import com.titankv.TitanKVClient;
import com.titankv.client.ClientConfig;

import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;

/**
 * Closed-loop load generator: each thread issues one request at a time with its own client.
 *
 * Every thread first writes its whole key space, so reads hit existing keys, then runs an
 * unmeasured warmup (JIT, connection pools) before the measured run. Latency is recorded per
 * operation and reported as percentiles.
 */
public class LoadGenerator {

    private final String[] hosts;
    private final int threads;
    private final int opsPerThread;
    private final int warmupOpsPerThread;
    private final double readRatio;
    private final int valueSize;
    private final int keysPerThread;

    private final LongAdder reads = new LongAdder();
    private final LongAdder readMisses = new LongAdder();
    private final LongAdder writes = new LongAdder();
    private final LongAdder errors = new LongAdder();

    public LoadGenerator(String[] hosts, int threads, int opsPerThread, int warmupOpsPerThread,
            double readRatio, int valueSize, int keysPerThread) {
        this.hosts = hosts;
        this.threads = threads;
        this.opsPerThread = opsPerThread;
        this.warmupOpsPerThread = warmupOpsPerThread;
        this.readRatio = readRatio;
        this.valueSize = valueSize;
        this.keysPerThread = keysPerThread;
    }

    public Result run() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CyclicBarrier start = new CyclicBarrier(threads + 1);
        CyclicBarrier measured = new CyclicBarrier(threads + 1);
        long[][] latencies = new long[threads][];
        Future<?>[] futures = new Future<?>[threads];

        for (int t = 0; t < threads; t++) {
            int thread = t;
            futures[t] = pool.submit(() -> {
                try (TitanKVClient client = createClient()) {
                    for (int k = 0; k < keysPerThread; k++) {
                        client.put(key(thread, k), randomValue());
                    }
                    start.await();
                    runOps(client, thread, warmupOpsPerThread, null);
                    measured.await();
                    long[] samples = new long[opsPerThread];
                    runOps(client, thread, opsPerThread, samples);
                    latencies[thread] = samples;
                }
                return null;
            });
        }

        System.out.println("Preloading " + threads * keysPerThread + " keys...");
        start.await();
        System.out.println("Warming up (" + threads * warmupOpsPerThread + " ops)...");
        measured.await();
        long startNanos = System.nanoTime();
        for (Future<?> future : futures) {
            future.get();
        }
        long elapsed = System.nanoTime() - startNanos;
        pool.shutdown();

        long[] all = Arrays.stream(latencies).flatMapToLong(Arrays::stream).filter(v -> v > 0).sorted().toArray();
        return new Result(reads.sum(), readMisses.sum(), writes.sum(), errors.sum(), elapsed, all);
    }

    private void runOps(TitanKVClient client, int thread, int count, long[] samples) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = 0; i < count; i++) {
            String key = key(thread, random.nextInt(keysPerThread));
            boolean read = random.nextDouble() < readRatio;
            long begin = System.nanoTime();
            boolean measured = samples != null;
            try {
                if (read) {
                    boolean miss = client.get(key).isEmpty();
                    if (measured) {
                        reads.increment();
                        if (miss) {
                            readMisses.increment();
                        }
                    }
                } else {
                    client.put(key, randomValue());
                    if (measured) {
                        writes.increment();
                    }
                }
                if (measured) {
                    samples[i] = System.nanoTime() - begin;
                }
            } catch (Exception e) {
                if (measured) {
                    errors.increment();
                }
            }
        }
    }

    private static String key(int thread, int index) {
        return "bench:" + thread + ":" + index;
    }

    private byte[] randomValue() {
        byte[] value = new byte[valueSize];
        ThreadLocalRandom.current().nextBytes(value);
        return value;
    }

    private TitanKVClient createClient() {
        return new TitanKVClient(ClientConfig.builder()
                .connectTimeoutMs(5000)
                .readTimeoutMs(10000)
                .maxConnectionsPerHost(2)
                .retryOnFailure(false)
                .build(), hosts);
    }

    public static final class Result {
        final long reads;
        final long readMisses;
        final long writes;
        final long errors;
        final long elapsedNanos;
        final long[] sortedLatencies;

        Result(long reads, long readMisses, long writes, long errors, long elapsedNanos, long[] sortedLatencies) {
            this.reads = reads;
            this.readMisses = readMisses;
            this.writes = writes;
            this.errors = errors;
            this.elapsedNanos = elapsedNanos;
            this.sortedLatencies = sortedLatencies;
        }

        double seconds() {
            return elapsedNanos / 1e9;
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
            System.out.printf("Duration:          %.2f s%n", seconds());
            System.out.printf("Throughput:        %,.0f ops/sec (%,.0f reads/sec, %,.0f writes/sec)%n",
                    ops / seconds(), reads / seconds(), writes / seconds());
            System.out.printf("Latency p50/p95/p99/max: %.3f / %.3f / %.3f / %.3f ms%n",
                    percentileMs(0.50), percentileMs(0.95), percentileMs(0.99), percentileMs(1.0));
        }
    }

    public static void main(String[] args) throws Exception {
        String[] hosts = {"localhost:9001"};
        int threads = 16;
        int ops = 20_000;
        int warmup = 2_000;
        double readRatio = 0.8;
        int valueSize = 100;
        int keys = 1_000;

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.equals("--help")) {
                printUsage();
                return;
            }
            if (i + 1 >= args.length) {
                throw new IllegalArgumentException("Missing value for " + arg);
            }
            String value = args[++i];
            switch (arg) {
                case "--hosts": hosts = value.split(","); break;
                case "--threads": threads = Integer.parseInt(value); break;
                case "--ops": ops = Integer.parseInt(value); break;
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
        System.out.println("Ops per thread:   " + ops + " (after " + warmup + " warmup)");
        System.out.println("Read ratio:       " + readRatio);
        System.out.println("Value size:       " + valueSize + " bytes");
        System.out.println("Keys per thread:  " + keys);
        System.out.println("CPUs:             " + Runtime.getRuntime().availableProcessors());
        System.out.println("-------------------------------------------");

        new LoadGenerator(hosts, threads, ops, warmup, readRatio, valueSize, keys).run().print();
    }

    private static void printUsage() {
        System.out.println("Usage: LoadGenerator [--hosts h1:p1,h2:p2] [--threads 16] [--ops 20000] [--warmup 2000]");
        System.out.println("                     [--read-ratio 0.8] [--value-size 100] [--keys 1000]");
    }
}
