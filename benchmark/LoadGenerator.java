package com.titankv.benchmark;

import com.titankv.TitanKVClient;
import com.titankv.client.ClientConfig;
import com.titankv.consistency.ConsistencyLevel;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

/**
 * Load generator with two modes. Each thread has its own client and one request in flight.
 *
 * <p><b>Closed loop</b> (default): a thread sends its next request as soon as the previous one
 * returns. This finds maximum throughput, but its latencies suffer from coordinated omission: while
 * the server is slow, the generator also slows down and stops sending, so the requests that would
 * have waited are never measured.
 *
 * <p><b>Open loop</b> ({@code --rate}): requests follow a fixed schedule of {@code rate} ops/sec
 * spread evenly across threads, and latency is measured from each request's <i>intended</i> send
 * time. A thread that falls behind sends its overdue requests immediately, without skipping any, so
 * the time they spent waiting shows up in the latencies. The time from actual send to response
 * (service time, which is what closed loop measures) is reported alongside.
 *
 * <p>Every thread first writes its whole key space, so reads hit existing keys, then runs an
 * unmeasured closed-loop warmup (JIT, connection pools) before the measured run.
 *
 * <p>Reads are also checked for correctness. Each value starts with an 8-byte sequence number that
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
    private final ConsistencyLevel consistency;
    /** Target ops/sec across all threads; 0 means closed loop. */
    private final double rate;

    private final LongAdder reads = new LongAdder();
    private final LongAdder readMisses = new LongAdder();
    private final LongAdder staleReads = new LongAdder();
    private final LongAdder writes = new LongAdder();
    private final LongAdder errors = new LongAdder();
    private final LongAdder preloadErrors = new LongAdder();
    private final LongAdder unsent = new LongAdder();
    private static final Map<String, LongAdder> ERROR_KINDS = new java.util.concurrent.ConcurrentHashMap<>();

    /** Start of the measured run, set by the barrier action before any thread is released. */
    private volatile long measuredStartNanos;

    public LoadGenerator(String[] hosts, int threads, int opsPerThread, int durationSeconds, int warmupOpsPerThread,
            double readRatio, int valueSize, int keysPerThread, boolean retry, ConsistencyLevel consistency,
            double rate) {
        if (valueSize < 8) {
            throw new IllegalArgumentException("--value-size must be at least 8 (values carry a sequence number)");
        }
        if (rate < 0) {
            throw new IllegalArgumentException("--rate must be positive");
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
        this.consistency = consistency;
        this.rate = rate;
    }

    private boolean openLoop() {
        return rate > 0;
    }

    /** Open loop: how many requests each thread sends in the measured run. */
    private long scheduledOpsPerThread() {
        return durationSeconds > 0 ? Math.max(1, Math.round(rate * durationSeconds / threads)) : opsPerThread;
    }

    /**
     * @param timeline print throughput and errors every second of the measured run
     */
    public Result run(boolean timeline) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CyclicBarrier start = new CyclicBarrier(threads + 1);
        CyclicBarrier measured = new CyclicBarrier(threads + 1, () -> measuredStartNanos = System.nanoTime());
        long[][] latencies = new long[threads][];
        long[][] serviceTimes = new long[threads][];
        Future<?>[] futures = new Future<?>[threads];
        AtomicBoolean stop = new AtomicBoolean();

        for (int t = 0; t < threads; t++) {
            int thread = t;
            futures[t] = pool.submit(() -> {
                Worker worker = new Worker(thread);
                try (TitanKVClient client = createClient()) {
                    for (int k = 0; k < keysPerThread; k++) {
                        try {
                            worker.write(client, key(thread, k));
                        } catch (Exception e) {
                            preloadErrors.increment(); // keep going: a dead thread would stall the barrier
                        }
                    }
                    start.await();
                    worker.runOps(client, warmupOpsPerThread, null, false);
                    measured.await();
                    if (openLoop()) {
                        long[][] samples = worker.runOpenLoop(client);
                        latencies[thread] = samples[0];
                        serviceTimes[thread] = samples[1];
                    } else {
                        latencies[thread] = worker.runMeasured(client, stop);
                        serviceTimes[thread] = latencies[thread];
                    }
                }
                return null;
            });
        }

        System.out.println("Preloading " + threads * keysPerThread + " keys...");
        start.await();
        if (preloadErrors.sum() > 0) {
            System.out.println("WARNING: " + preloadErrors.sum() + " preload writes failed; those keys may read as misses");
        }
        System.out.println("Warming up (" + threads * warmupOpsPerThread + " ops)...");
        measured.await();
        long startNanos = measuredStartNanos;

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
        if (durationSeconds > 0 && !openLoop()) {
            reporter.schedule(() -> stop.set(true), durationSeconds, TimeUnit.SECONDS);
        }
        for (Future<?> future : futures) {
            future.get();
        }
        long elapsed = System.nanoTime() - startNanos;
        reporter.shutdownNow();
        pool.shutdown();

        return new Result(rate, reads.sum(), readMisses.sum(), staleReads.sum(), writes.sum(), errors.sum(),
                unsent.sum(), elapsed, sortedPositive(latencies), sortedPositive(serviceTimes));
    }

    private static long[] sortedPositive(long[][] perThread) {
        return Arrays.stream(perThread).flatMapToLong(Arrays::stream).filter(v -> v > 0).sorted().toArray();
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
                long begin = System.nanoTime();
                if (op(client, true)) {
                    samples[count] = System.nanoTime() - begin;
                }
                count++;
            }
            return Arrays.copyOf(samples, count);
        }

        void runOps(TitanKVClient client, int count, long[] samples, boolean measured) {
            for (int i = 0; i < count; i++) {
                long begin = System.nanoTime();
                if (op(client, measured) && samples != null) {
                    samples[i] = System.nanoTime() - begin;
                }
            }
        }

        /**
         * Open loop. Request {@code i} of this thread is request {@code i * threads + thread} of one
         * global schedule at {@code rate} ops/sec, so the threads' sends interleave evenly. Returns
         * {latency from intended send time, service time from actual send time}.
         *
         * <p>If the thread is behind schedule it sends right away and keeps going; nothing is skipped.
         * Only if it is still sending past twice the planned run length plus 10 seconds does it give
         * up, and the requests it never sent are counted as unsent.
         */
        long[][] runOpenLoop(TitanKVClient client) {
            long count = scheduledOpsPerThread();
            double intervalNanos = 1e9 / rate;
            long start = measuredStartNanos;
            long plannedNanos = (long) (count * threads * intervalNanos);
            long deadline = start + 2 * plannedNanos + 10_000_000_000L;
            long[] latency = new long[(int) count];
            long[] service = new long[(int) count];
            for (int i = 0; i < count; i++) {
                long intended = start + (long) (((double) i * threads + thread) * intervalNanos);
                long now = System.nanoTime();
                while (now < intended) {
                    // Timed sleeps overshoot in proportion to their length (by about a quarter on
                    // macOS), and the overshoot would count as latency. Sleeping half the remaining
                    // time at a time keeps the last sleep, and so the overshoot, short.
                    long remaining = intended - now;
                    LockSupport.parkNanos(remaining > 100_000 ? remaining / 2 : remaining);
                    now = System.nanoTime();
                }
                if (now > deadline) {
                    unsent.add(count - i);
                    return new long[][] {Arrays.copyOf(latency, i), Arrays.copyOf(service, i)};
                }
                if (op(client, true)) {
                    long end = System.nanoTime();
                    latency[i] = end - intended;
                    service[i] = end - now;
                }
            }
            return new long[][] {latency, service};
        }

        /** Runs one random read or write; returns whether it succeeded. */
        boolean op(TitanKVClient client, boolean measured) {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            String key = key(thread, random.nextInt(keysPerThread));
            boolean read = random.nextDouble() < readRatio;
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
                return true;
            } catch (Exception e) {
                if (measured) {
                    errors.increment();
                    String kind = (read ? "read: " : "write: ") + String.valueOf(e.getMessage()).replaceAll("[0-9]+", "#");
                    ERROR_KINDS.computeIfAbsent(kind, k -> new LongAdder()).increment();
                }
                return false;
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
                .consistency(consistency)
                .build(), hosts);
    }

    public static final class Result {
        final double targetRate;
        final long reads;
        final long readMisses;
        final long staleReads;
        final long writes;
        final long errors;
        final long unsent;
        final long elapsedNanos;
        /** Closed loop: service time. Open loop: time from intended send to response. */
        final long[] sortedLatencies;
        /** Time from actual send to response; the same as sortedLatencies in closed loop. */
        final long[] sortedServiceTimes;

        Result(double targetRate, long reads, long readMisses, long staleReads, long writes, long errors, long unsent,
                long elapsedNanos, long[] sortedLatencies, long[] sortedServiceTimes) {
            this.targetRate = targetRate;
            this.reads = reads;
            this.readMisses = readMisses;
            this.staleReads = staleReads;
            this.writes = writes;
            this.errors = errors;
            this.unsent = unsent;
            this.elapsedNanos = elapsedNanos;
            this.sortedLatencies = sortedLatencies;
            this.sortedServiceTimes = sortedServiceTimes;
        }

        double seconds() {
            return elapsedNanos / 1e9;
        }

        double throughput() {
            return (reads + writes) / seconds();
        }

        double percentileMs(double p) {
            return percentileMs(sortedLatencies, p);
        }

        static double percentileMs(long[] sorted, double p) {
            if (sorted.length == 0) {
                return 0;
            }
            int index = (int) Math.min(sorted.length - 1, Math.ceil(p * sorted.length) - 1);
            return sorted[Math.max(0, index)] / 1e6;
        }

        void print() {
            boolean open = targetRate > 0;
            long ops = reads + writes;
            System.out.println();
            System.out.println("Results");
            System.out.println("-------------------------------------------");
            System.out.println("Mode:              " + (open ? "open loop (fixed rate)" : "closed loop"));
            System.out.printf("Operations:        %,d (%,d reads, %,d writes)%n", ops, reads, writes);
            System.out.printf("Errors:            %,d%n", errors);
            System.out.printf("Read misses:       %,d (should be 0: every key is preloaded)%n", readMisses);
            System.out.printf("Stale reads:       %,d (reads older than the thread's last acknowledged write)%n",
                    staleReads);
            System.out.printf("Duration:          %.2f s%n", seconds());
            if (open) {
                System.out.printf("Target rate:       %,.0f ops/sec%n", targetRate);
                System.out.printf("Achieved rate:     %,.0f ops/sec (%.1f%% of target)%n", throughput(),
                        throughput() / targetRate * 100);
                if (unsent > 0) {
                    System.out.printf("UNSENT:            %,d requests (generator fell too far behind; "
                            + "the latencies below leave them out)%n", unsent);
                }
            }
            System.out.printf("Throughput:        %,.0f ops/sec (%,.0f reads/sec, %,.0f writes/sec)%n",
                    ops / seconds(), reads / seconds(), writes / seconds());
            System.out.printf("Latency p50/p95/p99/p99.9/max: %.3f / %.3f / %.3f / %.3f / %.3f ms%s%n",
                    percentileMs(0.50), percentileMs(0.95), percentileMs(0.99), percentileMs(0.999),
                    percentileMs(1.0), open ? " (from intended send time)" : "");
            if (open) {
                System.out.printf("Service p50/p95/p99/p99.9/max: %.3f / %.3f / %.3f / %.3f / %.3f ms "
                        + "(from actual send time)%n", percentileMs(sortedServiceTimes, 0.50),
                        percentileMs(sortedServiceTimes, 0.95), percentileMs(sortedServiceTimes, 0.99),
                        percentileMs(sortedServiceTimes, 0.999), percentileMs(sortedServiceTimes, 1.0));
            }
            ERROR_KINDS.forEach((kind, count) -> System.out.printf("  error x%d: %s%n", count.sum(), kind));
            // Parsed by benchmark-suite.sh and benchmark-failover.sh: do not change these fields.
            System.out.printf("CSV,%.0f,%.3f,%.3f,%.3f,%.3f,%d,%d,%d,%d%n", throughput(), percentileMs(0.50),
                    percentileMs(0.95), percentileMs(0.99), percentileMs(1.0), errors, readMisses, staleReads, ops);
            System.out.printf("LATENCY,%s,%.0f,%.0f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%d,%d,%d,%d,%d%n",
                    open ? "open" : "closed", targetRate, throughput(),
                    percentileMs(0.50), percentileMs(0.95), percentileMs(0.99), percentileMs(0.999), percentileMs(1.0),
                    percentileMs(sortedServiceTimes, 0.50), percentileMs(sortedServiceTimes, 0.99),
                    percentileMs(sortedServiceTimes, 0.999), percentileMs(sortedServiceTimes, 1.0),
                    errors, readMisses, staleReads, ops, unsent);
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
        ConsistencyLevel consistency = null;
        double rate = 0;

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
                case "--consistency": consistency = ConsistencyLevel.valueOf(value.toUpperCase()); break;
                case "--rate":
                    rate = Double.parseDouble(value);
                    if (!(rate > 0)) {
                        throw new IllegalArgumentException("--rate must be a positive number of ops/sec");
                    }
                    break;
                default: throw new IllegalArgumentException("Unknown option " + arg);
            }
        }

        System.out.println("TitanKV Load Generator");
        System.out.println("-------------------------------------------");
        System.out.println("Hosts:            " + String.join(", ", hosts));
        System.out.println("Threads:          " + threads);
        System.out.println("Mode:             " + (rate > 0
                ? String.format("open loop at %,.0f ops/sec (%,.1f per thread)", rate, rate / threads)
                : "closed loop"));
        System.out.println(duration > 0
                ? "Duration:         " + duration + "s (after " + warmup + " warmup ops per thread)"
                : "Ops per thread:   " + ops + " (after " + warmup + " warmup)");
        System.out.println("Read ratio:       " + readRatio);
        System.out.println("Value size:       " + valueSize + " bytes");
        System.out.println("Keys per thread:  " + keys);
        System.out.println("Consistency:      " + (consistency != null ? consistency : "server default"));
        System.out.println("Client retries:   " + (retry ? "on (fails over to another node)" : "off"));
        System.out.println("CPUs:             " + Runtime.getRuntime().availableProcessors());
        System.out.println("-------------------------------------------");

        new LoadGenerator(hosts, threads, ops, duration, warmup, readRatio, valueSize, keys, retry, consistency, rate)
                .run(timeline).print();
    }

    private static void printUsage() {
        System.out.println("Usage: LoadGenerator [--hosts h1:p1,h2:p2] [--threads 16] [--ops 20000 | --duration SECONDS]");
        System.out.println("                     [--rate OPS_PER_SEC] [--warmup 2000] [--read-ratio 0.8]");
        System.out.println("                     [--value-size 100] [--keys 1000] [--consistency ONE|QUORUM|ALL]");
        System.out.println("                     [--retry] [--timeline]");
        System.out.println("  --rate         open loop: send OPS_PER_SEC in total, spread evenly over the threads, and");
        System.out.println("                 measure latency from each request's intended send time. A thread that");
        System.out.println("                 falls behind sends late rather than skipping, so the delay counts. With");
        System.out.println("                 --duration each thread sends rate*duration/threads requests, otherwise");
        System.out.println("                 --ops requests. --threads caps how many requests can be in flight.");
        System.out.println("                 Without --rate the run is closed loop: each thread sends its next request");
        System.out.println("                 when the previous one returns (maximum throughput; latencies understate");
        System.out.println("                 queueing because of coordinated omission).");
        System.out.println("  --consistency  level for every read and write (default: the server's)");
        System.out.println("  --retry        let clients fail over to another node on error");
        System.out.println("  --timeline     print TIMELINE,<second>,<ops>,<errors> every second");
        System.out.println("Output ends with two machine-readable lines:");
        System.out.println("  CSV,<ops/sec>,<p50>,<p95>,<p99>,<max>,<errors>,<misses>,<stale>,<ops>");
        System.out.println("  LATENCY,<closed|open>,<target ops/sec>,<achieved ops/sec>,<p50>,<p95>,<p99>,<p99.9>,<max>,");
        System.out.println("          <service p50>,<service p99>,<service p99.9>,<service max>,<errors>,<misses>,");
        System.out.println("          <stale>,<ops>,<unsent>");
        System.out.println("  Latencies are in ms. In open loop, CSV and the first five LATENCY latencies are from the");
        System.out.println("  intended send time; service times are from the actual send. Target is 0 in closed loop.");
    }
}
