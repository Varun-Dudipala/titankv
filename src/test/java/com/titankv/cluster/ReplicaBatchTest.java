package com.titankv.cluster;

import com.titankv.TitanKVClient;
import com.titankv.consistency.ConsistencyLevel;
import com.titankv.consistency.ReplicationManager;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Replica writes queue up when a peer answers slowly (it is waiting for its disk, or the link is
 * slow), and the coordinator then sends that peer several writes per request.
 */
@Tag("integration")
class ReplicaBatchTest {

    private static final int BASE_PORT = 24500;
    private static final int CLIENTS = 24;
    private static final int WRITES_PER_CLIENT = 10;

    @Test
    void writesToASlowReplicaAreSentTogetherAndAllArrive() throws Exception {
        try (TestCluster cluster = TestCluster.start(BASE_PORT, 3)) {
            // Every request from node 1 to the others takes 40 ms
            cluster.slowLink(0, 1, 40);
            cluster.slowLink(0, 2, 40);

            ExecutorService pool = Executors.newFixedThreadPool(CLIENTS);
            List<Future<?>> clients = new ArrayList<>();
            for (int c = 0; c < CLIENTS; c++) {
                int id = c;
                clients.add(pool.submit(() -> {
                    try (TitanKVClient client = cluster.client(cluster.address(0))) {
                        for (int i = 0; i < WRITES_PER_CLIENT; i++) {
                            String key = "batch:" + id + ":" + i;
                            client.put(key, ("value-" + i).getBytes(), 0, ConsistencyLevel.ALL);
                            if (i % 3 == 2) {
                                client.delete(key, ConsistencyLevel.ALL);
                            }
                        }
                    }
                    return null;
                }));
            }
            for (Future<?> client : clients) {
                client.get(60, TimeUnit.SECONDS);
            }
            pool.shutdown();

            // Acknowledged at ALL: every replica holds every write, values and tombstones alike
            for (int node = 0; node < 3; node++) {
                for (int c = 0; c < CLIENTS; c++) {
                    for (int i = 0; i < WRITES_PER_CLIENT; i++) {
                        String key = "batch:" + c + ":" + i;
                        var stored = cluster.node(node).getStore().getRaw(key);
                        assertThat(stored).as(key + " on node " + (node + 1)).isPresent();
                        assertThat(stored.get().isTombstone()).as(key + " deleted on node " + (node + 1))
                                .isEqualTo(i % 3 == 2);
                    }
                }
            }

            ReplicationManager coordinator = cluster.node(0).getReplicationManager();
            long writes = coordinator.getReplicaWritesSent();
            long requests = coordinator.getReplicaWriteRequests();
            System.out.printf("ReplicaBatchTest: %d replica writes sent in %d requests%n", writes, requests);
            // 240 puts and 72 deletes, to each of two peers
            assertThat(writes).isEqualTo(2L * (CLIENTS * WRITES_PER_CLIENT + CLIENTS * 3));
            assertThat(requests).as("requests carrying " + writes + " writes").isLessThan(writes / 2);
        }
    }

    @Test
    void anIdleClusterSendsEachWriteOnItsOwn() throws Exception {
        try (TestCluster cluster = TestCluster.start(BASE_PORT + 20, 3);
             TitanKVClient client = cluster.client(cluster.address(0))) {
            for (int i = 0; i < 50; i++) {
                client.put("single:" + i, "value", ConsistencyLevel.ALL);
            }
            ReplicationManager coordinator = cluster.node(0).getReplicationManager();
            assertThat(coordinator.getReplicaWritesSent()).isEqualTo(100);
            assertThat(coordinator.getReplicaWriteRequests()).as("no write waits to fill a batch").isEqualTo(100);
        }
    }
}
