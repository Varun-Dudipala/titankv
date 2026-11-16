package com.titankv.cluster;

import com.titankv.TitanKVClient;
import com.titankv.TitanKVServer;
import com.titankv.consistency.ConsistencyLevel;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The client asks each node for its id and builds the cluster's own ring from them, so the node
 * it sends a request to first is a replica of the key. Any node can coordinate any request, so
 * this is about cost, not correctness: a coordinator that holds the key answers from its own copy,
 * and one that does not has to ask the replicas.
 */
@Tag("integration")
class ClientRoutingTest {

    private static final int BASE_PORT = 24800;

    private static double replicaReads(TestCluster cluster) {
        double total = 0;
        for (int i = 0; i < cluster.size(); i++) {
            total += cluster.node(i).getMetrics().getRegistry().get("titankv.replica.ops")
                    .tag("operation", "read").counter().count();
        }
        return total;
    }

    /**
     * With five nodes and three replicas, a node picked without knowing the ring holds a given
     * key three times in five. Here every request must go to the key's first replica, and a read
     * at ONE must then need no request between nodes at all.
     */
    @Test
    void clientSendsEachRequestToTheFirstReplicaOfItsKey() throws Exception {
        try (TestCluster cluster = TestCluster.start(BASE_PORT, 5);
             TitanKVClient client = cluster.client()) {
            for (int i = 0; i < 300; i++) {
                client.put("routed-" + i, "value-" + i, ConsistencyLevel.ALL);
            }

            for (int i = 0; i < 300; i++) {
                String key = "routed-" + i;
                assertThat(client.coordinatorFor(key)).as(key)
                        .isEqualTo(cluster.address(cluster.replicasOf(key, 0).get(0)));
            }
            double before = replicaReads(cluster);
            for (int i = 0; i < 300; i++) {
                assertThat(client.getString("routed-" + i, ConsistencyLevel.ONE)).contains("value-" + i);
            }
            assertThat(replicaReads(cluster) - before).as("reads one node had to ask another for").isZero();
        }
    }

    /**
     * A node that is down when the client starts cannot say what its id is. It is placed by its
     * address until the first request it answers, and by its id from then on.
     */
    @Test
    void nodeThatWasDownIsPlacedByItsIdOnceItAnswers() throws Exception {
        try (TestCluster cluster = TestCluster.start(BASE_PORT + 10, 5)) {
            cluster.stopNode(4);
            cluster.awaitAliveCount(0, 4, 10_000);
            try (TitanKVClient client = cluster.client()) {
                for (int i = 0; i < 100; i++) {
                    client.put("late-" + i, "value-" + i);
                }

                TitanKVServer back = cluster.startNode(4);
                cluster.awaitConverged(20_000);
                assertThat(back.getClusterManager().isReady()).isTrue();
                // The client's circuit breaker keeps a node that was failing at the back of the
                // order for 5 seconds; after that requests go to it again, and the first one it
                // answers is followed by the question
                TestCluster.awaitCondition(() -> {
                    try {
                        for (int i = 0; i < 100; i++) {
                            client.getString("late-" + i);
                        }
                    } catch (java.io.IOException e) {
                        return false;
                    }
                    for (int i = 0; i < 100; i++) {
                        String key = "late-" + i;
                        if (!client.coordinatorFor(key).equals(cluster.address(cluster.replicasOf(key, 0).get(0)))) {
                            return false;
                        }
                    }
                    return true;
                }, 20_000, "every key's first node to be its first replica again");
            }
        }
    }
}
