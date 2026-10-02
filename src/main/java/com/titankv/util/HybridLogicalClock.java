package com.titankv.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Hybrid logical clock (Kulkarni et al., 2014) used to version writes.
 *
 * A timestamp packs wall-clock milliseconds in the high 48 bits and a logical counter in the low
 * 16 bits, so it stays close to real time but can still order events whose clocks disagree. Every
 * timestamp this clock issues is greater than every timestamp it has issued or observed. Observing
 * the timestamps of replicated writes, and those clients have seen, is what keeps last-write-wins
 * from discarding a newer write whose coordinator's clock runs behind.
 *
 * Remote timestamps more than {@link #MAX_FORWARD_DRIFT_MS} ahead of local time are not adopted,
 * so one node with a badly wrong clock cannot drag every clock into the future.
 */
public final class HybridLogicalClock {

    private static final Logger logger = LoggerFactory.getLogger(HybridLogicalClock.class);

    private static final int LOGICAL_BITS = 16;
    private static final long LOGICAL_MASK = (1L << LOGICAL_BITS) - 1;
    public static final long MAX_FORWARD_DRIFT_MS = 60_000;

    private final AtomicLong last = new AtomicLong();
    private volatile long skewMillis;

    /**
     * @return a timestamp greater than any previously issued or observed by this clock
     */
    public long next() {
        long physical = encode(now());
        return last.updateAndGet(previous -> Math.max(previous + 1, physical));
    }

    /**
     * Merge a timestamp received from elsewhere, so later local timestamps sort after it.
     */
    public void observe(long timestamp) {
        if (timestamp <= 0) {
            return;
        }
        if (physicalMillis(timestamp) > now() + MAX_FORWARD_DRIFT_MS) {
            logger.warn("Ignoring timestamp {}ms ahead of the local clock", physicalMillis(timestamp) - now());
            return;
        }
        last.accumulateAndGet(timestamp, Math::max);
    }

    /**
     * @return the wall-clock milliseconds a timestamp was issued at
     */
    public static long physicalMillis(long timestamp) {
        return timestamp >>> LOGICAL_BITS;
    }

    /**
     * @return the smallest timestamp at the given wall-clock milliseconds
     */
    public static long encode(long millis) {
        return millis << LOGICAL_BITS;
    }

    /**
     * Fault injection for tests: shift this clock's view of wall time.
     */
    public void setSkewMillis(long skewMillis) {
        this.skewMillis = skewMillis;
    }

    private long now() {
        return System.currentTimeMillis() + skewMillis;
    }
}
