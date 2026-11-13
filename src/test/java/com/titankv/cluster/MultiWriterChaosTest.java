package com.titankv.cluster;

import com.titankv.TitanKVClient;
import com.titankv.cluster.HistoryChecker.Kind;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Eight clients read, write and delete the same twenty keys at QUORUM on a 5-node production-mode
 * cluster while a {@link Nemesis} partitions it, crashes nodes and loses replies. Every operation
 * is recorded, and the whole history is then checked ({@link HistoryChecker}): no read may return
 * a value, or a missing key, that an acknowledged write had already replaced before the read
 * started. Afterwards all three replicas of every key must hold the same entry.
 *
 * The other chaos tests give each key one writer. One client's own clock then orders that key's
 * writes whatever the cluster does, so they cannot see a write from one client lost to another's,
 * a delete undone, or a failed write surfacing over a later one. Here the order of writes is the
 * cluster's to get right.
 */
@Tag("integration")
class MultiWriterChaosTest {

    private static final int NODES = 5;
    private static final int CLIENTS = 8;
    private static final int KEYS = 20;
    private static final long CHAOS_MILLIS = 40_000;
    private static final long[] NODE_SKEW_MILLIS = {-20_000, 12_000, 0, 20_000, -7_000};
    private static final long[] CLIENT_SKEW_MILLIS = {-10_000, 9_000, 0, -4_000, 10_000, 3, -7_000, 6_000};

    private TestCluster cluster;

    @BeforeEach
    void productionMode() throws Exception {
        System.setProperty("titankv.dev.mode", "false");
        System.setProperty("titankv.cluster.secret", "multi-writer-chaos-secret");
        System.setProperty("titankv.data.dir", Files.createTempDirectory("titankv-multi-writer").toString());
        System.setProperty("titankv.anti.entropy.interval.ms", "3000");
    }

    @AfterEach
    void stopCluster() {
        if (cluster != null) {
            cluster.close();
        }
        System.clearProperty("titankv.cluster.secret");
        System.clearProperty("titankv.data.dir");
        System.clearProperty("titankv.anti.entropy.interval.ms");
        System.setProperty("titankv.dev.mode", "true");
    }

    @Test
    void sharedKeysKeepTheirHistoryUnderFaults() throws Exception {
        cluster = TestCluster.start(24200, NODES);
        runChaos("shared", 17, null);
    }

    /**
     * The same with node clocks up to 20 seconds wrong and client clocks up to 20 seconds apart:
     * which write wins must still follow the order the writes were acknowledged in.
     */
    @Test
    void sharedKeysKeepTheirHistoryWithSkewedClocks() throws Exception {
        cluster = TestCluster.start(24300, NODES);
        for (int i = 0; i < NODES; i++) {
            cluster.node(i).getClusterManager().getClock().setSkewMillis(NODE_SKEW_MILLIS[i]);
        }
        runChaos("skewed", 23, CLIENT_SKEW_MILLIS);
    }

    private void runChaos(String name, long seed, long[] clientSkewMillis) throws Exception {
        RegisterWorkload workload = new RegisterWorkload(name, cluster.addresses(), CLIENTS, KEYS, clientSkewMillis);
        workload.start();
        List<String> faults;
        try {
            faults = Nemesis.run(cluster, seed, CHAOS_MILLIS, restarted -> {
                if (clientSkewMillis != null) {
                    cluster.node(restarted).getClusterManager().getClock().setSkewMillis(NODE_SKEW_MILLIS[restarted]);
                }
            });
        } finally {
            workload.stop();
        }
        cluster.heal();
        cluster.awaitConverged(30_000);
        try (TitanKVClient client = cluster.client()) {
            workload.readFinalState(client);
        }

        HistoryChecker history = workload.history();
        List<String> violations = history.check();
        List<String> diverged = cluster.awaitReplicasAgree(workload.keys(), 45_000);
        System.out.printf("MultiWriterChaosTest (%s): %d faults; %d writes and %d deletes acknowledged, "
                        + "%d with unknown outcome, %d reads checked%n  faults: %s%n", name, faults.size(),
                history.count(Kind.PUT, true), history.count(Kind.DELETE, true),
                history.count(Kind.PUT, false) + history.count(Kind.DELETE, false),
                history.count(Kind.READ, true), faults);

        assertThat(faults.size()).isGreaterThanOrEqualTo(Nemesis.kinds());
        assertThat(history.count(Kind.PUT, true)).isGreaterThan(1_000);
        assertThat(history.count(Kind.DELETE, true)).isGreaterThan(100);
        assertThat(violations).as("reads that an acknowledged write had already made impossible").isEmpty();
        assertThat(diverged).as("replicas that still differ 45s after the network healed").isEmpty();
    }
}
