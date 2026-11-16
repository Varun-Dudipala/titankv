package com.titankv.cluster;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A node with seeds must not report ready at any moment while it starts. The server accepts
 * connections before its cluster manager has started, so a request can arrive in between.
 */
class StartupReadinessTest {

    @Test
    void nodeWithSeedsIsNeverReadyWhileItStarts() throws Exception {
        AtomicReference<ClusterManager> starting = new AtomicReference<>();
        AtomicBoolean done = new AtomicBoolean();
        AtomicInteger sawReady = new AtomicInteger();
        Thread watcher = new Thread(() -> {
            while (!done.get()) {
                ClusterManager manager = starting.get();
                if (manager != null && manager.isReady()) {
                    sawReady.incrementAndGet();
                }
            }
        }, "readiness-watcher");
        watcher.start();
        try {
            for (int i = 0; i < 12; i++) {
                ClusterManager manager = new ClusterManager(new Node("starting-" + i, "localhost", 18400 + i),
                        (String) null);
                starting.set(manager);
                // A seed name that has to be looked up makes starting take longer, as it would
                // with real host names
                manager.start("no-such-host-" + i + ".invalid:9001,127.0.0.1:1");
                assertThat(manager.isReady()).as("ready with no member heard from").isFalse();
                starting.set(null);
                manager.stop(false);
            }
        } finally {
            done.set(true);
            watcher.join(5_000);
        }
        assertThat(sawReady.get()).as("times a starting node reported ready").isZero();
    }
}
