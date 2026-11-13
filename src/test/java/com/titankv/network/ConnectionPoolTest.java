package com.titankv.network;

import com.titankv.network.ConnectionPool.PooledConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The per-host connection limit, with more threads than connections. The listening socket never
 * accepts: connections complete in its backlog, which is all the pool needs.
 */
class ConnectionPoolTest {

    private ServerSocket server;
    private String host;
    private final List<ConnectionPool> pools = new ArrayList<>();

    @BeforeEach
    void listen() throws IOException {
        server = new ServerSocket(0);
        host = "127.0.0.1:" + server.getLocalPort();
    }

    @AfterEach
    void close() throws IOException {
        pools.forEach(ConnectionPool::close);
        server.close();
    }

    private ConnectionPool pool(int maxConnections, int waitMs) {
        ConnectionPool pool = new ConnectionPool(maxConnections, waitMs, 5_000);
        pools.add(pool);
        return pool;
    }

    /**
     * A thread waiting at the limit must get the connection another thread returns. It used to
     * wait on a permit that was only freed when a connection was closed, so it timed out with
     * "pool exhausted" while the connection sat idle.
     */
    @Test
    void aWaitingThreadGetsTheConnectionAnotherThreadReturns() throws Exception {
        ConnectionPool pool = pool(1, 10_000);
        PooledConnection first = pool.acquire(host);

        ExecutorService waiter = Executors.newSingleThreadExecutor();
        try {
            CountDownLatch waiting = new CountDownLatch(1);
            Future<PooledConnection> second = waiter.submit(() -> {
                waiting.countDown();
                return pool.acquire(host);
            });
            assertThat(waiting.await(5, TimeUnit.SECONDS)).isTrue();
            Thread.sleep(100); // let it block at the limit
            assertThat(second.isDone()).isFalse();

            pool.release(first);

            PooledConnection reused = second.get(2, TimeUnit.SECONDS);
            assertThat(reused).isSameAs(first);
            assertThat(reused.isReused()).isTrue();
            assertThat(pool.getTotalConnections()).isEqualTo(1);
            pool.release(reused);
        } finally {
            waiter.shutdownNow();
        }
    }

    @Test
    void moreThreadsThanConnectionsShareThemWithoutExceedingTheLimit() throws Exception {
        int limit = 2;
        ConnectionPool pool = pool(limit, 10_000);
        AtomicInteger inUse = new AtomicInteger();
        AtomicInteger mostInUse = new AtomicInteger();

        ExecutorService threads = Executors.newFixedThreadPool(8);
        try {
            List<Future<?>> work = new ArrayList<>();
            for (int t = 0; t < 8; t++) {
                work.add(threads.submit(() -> {
                    for (int i = 0; i < 200; i++) {
                        PooledConnection conn = pool.acquire(host);
                        mostInUse.accumulateAndGet(inUse.incrementAndGet(), Math::max);
                        Thread.yield();
                        inUse.decrementAndGet();
                        pool.release(conn);
                    }
                    return null;
                }));
            }
            for (Future<?> future : work) {
                future.get(30, TimeUnit.SECONDS); // fails if any acquire timed out
            }
        } finally {
            threads.shutdownNow();
        }

        assertThat(mostInUse.get()).isLessThanOrEqualTo(limit);
        assertThat(pool.getTotalConnections()).isLessThanOrEqualTo(limit);
        assertThat(pool.getIdleConnections()).isEqualTo(pool.getTotalConnections());
    }

    @Test
    void theLimitHoldsAndFailsAfterTheWait() throws Exception {
        ConnectionPool pool = pool(1, 200);
        PooledConnection only = pool.acquire(host);

        assertThatThrownBy(() -> pool.acquire(host))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Connection pool exhausted");
        pool.release(only);
    }

    @Test
    void invalidatingOrReleasingTwiceFreesOnlyOneSlot() throws Exception {
        ConnectionPool pool = pool(1, 200);
        PooledConnection broken = pool.acquire(host);
        pool.invalidate(broken);
        pool.invalidate(broken);
        pool.release(broken);
        assertThat(pool.getTotalConnections()).isZero();

        PooledConnection replacement = pool.acquire(host);
        assertThat(replacement).isNotSameAs(broken);
        assertThat(replacement.isReused()).isFalse();
        // Had the extra calls freed extra slots, this would open a second connection
        assertThatThrownBy(() -> pool.acquire(host)).hasMessageContaining("Connection pool exhausted");
        pool.release(replacement);
        assertThat(pool.getTotalConnections()).isEqualTo(1);
    }

    @Test
    void evictingIdleConnectionsLeavesRoomToReconnect() throws Exception {
        ConnectionPool pool = pool(2, 200);
        PooledConnection a = pool.acquire(host);
        PooledConnection b = pool.acquire(host);
        pool.release(a);
        pool.release(b);
        assertThat(pool.getIdleConnections()).isEqualTo(2);

        pool.evictIdle(host);
        assertThat(pool.getTotalConnections()).isZero();

        PooledConnection fresh = pool.acquire(host);
        PooledConnection alsoFresh = pool.acquire(host);
        assertThat(fresh.isReused()).isFalse();
        assertThat(alsoFresh.isReused()).isFalse();
        pool.release(fresh);
        pool.release(alsoFresh);
    }

    @Test
    void aConnectionClosedWhileIdleIsReplaced() throws Exception {
        ConnectionPool pool = pool(1, 200);
        PooledConnection stale = pool.acquire(host);
        pool.release(stale);
        stale.getChannel().close();

        PooledConnection fresh = pool.acquire(host);
        assertThat(fresh).isNotSameAs(stale);
        assertThat(fresh.isOpen()).isTrue();
        assertThat(pool.getTotalConnections()).isEqualTo(1);
        pool.release(fresh);
    }
}
