package com.titankv.benchmark;

import com.titankv.TitanKVClient;
import com.titankv.client.ClientConfig;

import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;

/**
 * Multi-threaded load generator for benchmarking TitanKV.
 */
public class LoadGenerator {

    private final String[] hosts;
    private final int opsPerThread;
    private final int numThreads;
    private final double readRatio;
    private final int valueSize;

    // Metrics
    private final LongAdder totalOps = new LongAdder();
    private final LongAdder readOps = new LongAdder();
    private final LongAdder writeOps = new LongAdder();
    private final LongAdder totalLatencyNanos = new LongAdder();
    private final LongAdder errors = new LongAdder();

    /**
     * Create a load generator.
     *
     * @param hosts        cluster hosts in host:port format
     * @param threads      number of concurrent threads
     * @param opsPerThread operations per thread
     * @param readRatio    ratio of reads (0.0 to 1.0)
     */
    public LoadGenerator(String[] hosts, int threads, int opsPerThread, double readRatio) {
        this(hosts, threads, opsPerThread, readRatio, 100);
    }

    /**
     * Create a load generator with custom value size.
     *
     * @param hosts        cluster hosts
     * @param threads      number of concurrent threads
     * @param opsPerThread operations per thread
     * @param readRatio    ratio of reads (0.0 to 1.0)
     * @param valueSize    size of values in bytes
     */
    public LoadGenerator(String[] hosts, int threads, int opsPerThread, double readRatio, int valueSize) {
        this.hosts = hosts;
        this.numThreads = threads;
        this.opsPerThread = opsPerThread;
        this.readRatio = readRatio;
        this.valueSize = valueSize;
    }

    /**
     * Run the benchmark.
     *
     * @return benchmark results
     * @throws InterruptedException if interrupted
     */
    public BenchmarkResult run() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(numThreads);

        // Pre-populate some data for reads
        System.out.println("Pre-populating data...");
        try (TitanKVClient client = createClient()) {
            for (int i = 0; i < 1000; i++) {
                client.put("preload:" + i, generateValue());
            }
        } catch (Exception e) {
            System.err.println("Pre-population failed: " + e.getMessage());
        }

        // Reset counters
        totalOps.reset();
        readOps.reset();
        writeOps.reset();
        totalLatencyNanos.reset();
        errors.reset();

        System.out.println("Starting benchmark with " + numThreads + " threads...");

        // Submit worker tasks
        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await(); // Wait for signal to start
                    runWorker(threadId);
                } catch (Exception e) {
                    errors.increment();
                } finally {
                    endLatch.countDown();
                }
            });
        }

        // Start timing
        long startTime = System.nanoTime();
        startLatch.countDown(); // Signal all threads to start

        // Wait for completion
        endLatch.await();
        long endTime = System.nanoTime();

        executor.shutdown();

        return new BenchmarkResult(
            totalOps.sum(),
            readOps.sum(),
            writeOps.sum(),
            errors.sum(),
            endTime - startTime,
            totalLatencyNanos.sum()
        );
    }

    private void runWorker(int threadId) {
        TitanKVClient client = createClient();
        ThreadLocalRandom random = ThreadLocalRandom.current();

        try {
            for (int i = 0; i < opsPerThread; i++) {
                String key = "key:" + threadId + ":" + (i % 1000);
                byte[] value = generateValue();

                long opStart = System.nanoTime();

                try {
                    if (random.nextDouble() < readRatio) {
                        // Read operation
                        client.get(key);
                        readOps.increment();
                    } else {
                        // Write operation
                        client.put(key, value);
                        writeOps.increment();
                    }

                    long latency = System.nanoTime() - opStart;
                    totalLatencyNanos.add(latency);
                    totalOps.increment();

                } catch (Exception e) {
                    errors.increment();
                }
            }
        } finally {
            client.close();
        }
    }

    private TitanKVClient createClient() {
        ClientConfig config = ClientConfig.builder()
            .connectTimeoutMs(5000)
            .readTimeoutMs(10000)
            .maxConnectionsPerHost(5)
            .retryOnFailure(false)
            .build();
        return new TitanKVClient(config, hosts);
    }

    private byte[] generateValue() {
        byte[] value = new byte[valueSize];
        ThreadLocalRandom.current().nextBytes(value);
        return value;
    }

    /**
     * Benchmark result class.
     */
    public static class BenchmarkResult {
        public final long totalOps;
        public final long readOps;
        public final long writeOps;
        public final long errors;
        public final long durationNanos;
        public final long totalLatencyNanos;

        public BenchmarkResult(long totalOps, long readOps, long writeOps,
                               long errors, long durationNanos, long totalLatencyNanos) {
            this.totalOps = totalOps;
            this.readOps = readOps;
            this.writeOps = writeOps;
            this.errors = errors;
            this.durationNanos = durationNanos;
            this.totalLatencyNanos = totalLatencyNanos;
        }

        public double getOpsPerSecond() {
            return totalOps / (durationNanos / 1_000_000_000.0);
        }

        public double getReadOpsPerSecond() {
            return readOps / (durationNanos / 1_000_000_000.0);
        }

        public double getWriteOpsPerSecond() {
            return writeOps / (durationNanos / 1_000_000_000.0);
        }

        public double getAvgLatencyMs() {
            return totalOps > 0 ? (totalLatencyNanos / (double) totalOps) / 1_000_000.0 : 0;
        }

        public double getDurationSeconds() {
            return durationNanos / 1_000_000_000.0;
        }

        public void print() {
            System.out.println();
            System.out.println("════════════════════════════════════════════════════════════");
            System.out.println("              TitanKV Benchmark Results                      ");
            System.out.println("════════════════════════════════════════════════════════════");
            System.out.printf("Total Operations:    %,d%n", totalOps);
            System.out.printf("  - Reads:           %,d%n", readOps);
            System.out.printf("  - Writes:          %,d%n", writeOps);
            System.out.printf("Errors:              %,d%n", errors);
            System.out.println("────────────────────────────────────────────────────────────");
            System.out.printf("Duration:            %.2f seconds%n", getDurationSeconds());
            System.out.printf("Total Throughput:    %,.0f ops/sec%n", getOpsPerSecond());
            System.out.printf("  - Read Throughput: %,.0f ops/sec%n", getReadOpsPerSecond());
            System.out.printf("  - Write Throughput:%,.0f ops/sec%n", getWriteOpsPerSecond());
            System.out.printf("Avg Latency:         %.3f ms%n", getAvgLatencyMs());
            System.out.println("════════════════════════════════════════════════════════════");
            System.out.println();
        }
    }

    /**
     * Main entry point.
     */
    public static void main(String[] args) throws Exception {
        // Default configuration
        String[] cluster = {"localhost:9001"};
        int threads = 50;
        int opsPerThread = 1000;
        double readRatio = 0.8;
        int valueSize = 100;

        // Parse arguments
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--hosts":
                case "-h":
                    if (i + 1 < args.length) {
                        cluster = args[++i].split(",");
                    }
                    break;
                case "--threads":
                case "-t":
                    if (i + 1 < args.length) {
                        threads = Integer.parseInt(args[++i]);
                    }
                    break;
                case "--ops":
                case "-o":
                    if (i + 1 < args.length) {
                        opsPerThread = Integer.parseInt(args[++i]);
                    }
                    break;
                case "--read-ratio":
                case "-r":
                    if (i + 1 < args.length) {
                        readRatio = Double.parseDouble(args[++i]);
                    }
                    break;
                case "--value-size":
                case "-v":
                    if (i + 1 < args.length) {
                        valueSize = Integer.parseInt(args[++i]);
                    }
                    break;
                case "--help":
                    printUsage();
                    return;
            }
        }

        System.out.println("TitanKV Load Generator");
        System.out.println("─────────────────────────────────────");
        System.out.println("Hosts:          " + String.join(", ", cluster));
        System.out.println("Threads:        " + threads);
        System.out.println("Ops/Thread:     " + opsPerThread);
        System.out.println("Total Ops:      " + (threads * opsPerThread));
        System.out.println("Read Ratio:     " + (readRatio * 100) + "%");
        System.out.println("Value Size:     " + valueSize + " bytes");
        System.out.println("─────────────────────────────────────");

        LoadGenerator generator = new LoadGenerator(cluster, threads, opsPerThread, readRatio, valueSize);
        BenchmarkResult result = generator.run();
        result.print();
    }

    private static void printUsage() {
        System.out.println("TitanKV Load Generator");
        System.out.println();
        System.out.println("Usage: LoadGenerator [options]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  -h, --hosts <hosts>       Comma-separated host:port list (default: localhost:9001)");
        System.out.println("  -t, --threads <n>         Number of concurrent threads (default: 50)");
        System.out.println("  -o, --ops <n>             Operations per thread (default: 1000)");
        System.out.println("  -r, --read-ratio <ratio>  Read ratio 0.0-1.0 (default: 0.8)");
        System.out.println("  -v, --value-size <bytes>  Value size in bytes (default: 100)");
        System.out.println("      --help                Show this help");
    }
}
