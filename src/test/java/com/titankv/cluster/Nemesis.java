package com.titankv.cluster;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.IntConsumer;

/**
 * Keeps breaking a {@link TestCluster} while a test's workload runs. Every kind of fault happens
 * at least once, in an order chosen by the seed, and then they repeat:
 *
 * <ul>
 *   <li>one node cut off from the others;</li>
 *   <li>the cluster split in two;</li>
 *   <li>a link cut in one direction;</li>
 *   <li>a node crashed and restarted;</li>
 *   <li>a node crashed while another is cut off, restarting into the partition;</li>
 *   <li>a node cut off, left to take writes it cannot replicate, then crashed;</li>
 *   <li>replies lost after the request was carried out, between nodes and to clients.</li>
 * </ul>
 *
 * Cut links either refuse requests at once or swallow them until a timeout. Some faults end
 * before the failure detector notices (3s), some last past it.
 */
final class Nemesis {

    private static final int KINDS = 7;

    private Nemesis() {
    }

    /**
     * Inject faults for at least {@code millis}, and until every kind has happened once. The
     * cluster is healed, with every node running, when this returns.
     *
     * @param afterRestart called with the index of each node that was restarted, for per-node
     *                     state a test has to set again (a clock skew, say)
     * @return what was done, for the test's output
     */
    static List<String> run(TestCluster cluster, long seed, long millis, IntConsumer afterRestart)
            throws Exception {
        int nodes = cluster.size();
        Random random = new Random(seed);
        List<String> faults = new ArrayList<>();
        List<Integer> kinds = new ArrayList<>();
        for (int kind = 0; kind < KINDS; kind++) {
            kinds.add(kind);
        }
        Collections.shuffle(kinds, random);

        long end = System.currentTimeMillis() + millis;
        while (System.currentTimeMillis() < end || faults.size() < KINDS) {
            cluster.faults().setBlackholeMillis(random.nextBoolean() ? 0 : 400);
            int crashed = -1;
            String fault;
            switch (kinds.get(faults.size() % KINDS)) {
                case 0: {
                    int node = random.nextInt(nodes);
                    cluster.isolate(node);
                    fault = "isolate " + (node + 1);
                    break;
                }
                case 1: {
                    List<Integer> order = new ArrayList<>();
                    for (int i = 0; i < nodes; i++) {
                        order.add(i);
                    }
                    Collections.shuffle(order, random);
                    int[] small = {order.get(0), order.get(1)};
                    int[] large = new int[nodes - 2];
                    for (int i = 2; i < nodes; i++) {
                        large[i - 2] = order.get(i);
                    }
                    cluster.partition(small, large);
                    fault = "split " + (small[0] + 1) + "," + (small[1] + 1) + " | rest";
                    break;
                }
                case 2: {
                    int from = random.nextInt(nodes);
                    int to = (from + 1 + random.nextInt(nodes - 1)) % nodes;
                    cluster.cutOneWay(from, to);
                    fault = "cut " + (from + 1) + " -> " + (to + 1);
                    break;
                }
                case 3: {
                    crashed = random.nextInt(nodes);
                    cluster.crashNode(crashed);
                    fault = "crash " + (crashed + 1);
                    break;
                }
                case 4: {
                    int node = random.nextInt(nodes);
                    crashed = (node + 1 + random.nextInt(nodes - 1)) % nodes;
                    cluster.isolate(node);
                    cluster.crashNode(crashed);
                    fault = "isolate " + (node + 1) + " + crash " + (crashed + 1);
                    break;
                }
                case 5: {
                    crashed = random.nextInt(nodes);
                    cluster.isolate(crashed);
                    Thread.sleep(300 + random.nextInt(1_500));
                    cluster.crashNode(crashed);
                    fault = "isolate then crash " + (crashed + 1);
                    break;
                }
                default: {
                    cluster.faults().loseReplies(0.3, 0.2);
                    fault = "lose 30% of replies between nodes, 20% to clients";
                    break;
                }
            }
            long hold = random.nextInt(3) == 0 ? 4_000 + random.nextInt(3_000) : 500 + random.nextInt(2_000);
            faults.add(fault + " for " + hold + "ms");
            Thread.sleep(hold);
            if (crashed >= 0) {
                cluster.startNode(crashed);
                afterRestart.accept(crashed);
                Thread.sleep(500 + random.nextInt(1_000));
            }
            cluster.heal();
            Thread.sleep(1_000 + random.nextInt(2_000));
        }
        return faults;
    }

    static int kinds() {
        return KINDS;
    }
}
