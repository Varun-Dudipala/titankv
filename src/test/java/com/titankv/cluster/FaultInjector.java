package com.titankv.cluster;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The network faults a {@link TestCluster} can inject between its nodes: links cut in one or both
 * directions (a partition) and links with added latency. Nodes are identified by port, since every
 * node of a test cluster runs on localhost.
 *
 * A cut link drops gossip datagrams and fails node-to-node requests. By default a request fails at
 * once, like a refused connection; with {@link #setBlackholeMillis} it first hangs for that long,
 * like packets that are silently dropped until the caller's timeout. {@link #loseReplies} loses
 * answers to requests that were carried out, between nodes and to clients.
 */
public final class FaultInjector implements LinkFilter {

    private final Set<Long> cut = ConcurrentHashMap.newKeySet();
    private final Map<Long, Long> latencyMillis = new ConcurrentHashMap<>();
    private volatile long blackholeMillis;
    private volatile double replyLossRate;
    private volatile double clientReplyLossRate;

    private static long link(int fromPort, int toPort) {
        return ((long) fromPort << 32) | toPort;
    }

    /** Cut traffic from one node to another; the opposite direction is unaffected. */
    public void cut(int fromPort, int toPort) {
        cut.add(link(fromPort, toPort));
    }

    /** Cut every link between the two groups, in both directions. */
    public void partition(Collection<Integer> portsA, Collection<Integer> portsB) {
        for (int a : portsA) {
            for (int b : portsB) {
                cut(a, b);
                cut(b, a);
            }
        }
    }

    /** Add latency to every request one node sends another. */
    public void delay(int fromPort, int toPort, long millis) {
        latencyMillis.put(link(fromPort, toPort), millis);
    }

    /**
     * @param millis how long a request over a cut link hangs before it fails; 0 fails it at once
     */
    public void setBlackholeMillis(long millis) {
        this.blackholeMillis = millis;
    }

    /**
     * Lose this share of the replies between nodes, and of the replies to clients, after the
     * request was carried out: the sender sees a failure for something that happened.
     */
    public void loseReplies(double betweenNodes, double toClients) {
        this.replyLossRate = betweenNodes;
        this.clientReplyLossRate = toClients;
    }

    /** Restore every link. */
    public void heal() {
        cut.clear();
        latencyMillis.clear();
        replyLossRate = 0;
        clientReplyLossRate = 0;
    }

    public boolean isCut(int fromPort, int toPort) {
        return cut.contains(link(fromPort, toPort));
    }

    @Override
    public boolean allowsGossip(Node from, Node to) {
        return !isCut(from.getPort(), to.getPort());
    }

    @Override
    public void beforeRequest(Node from, Node to) throws IOException {
        if (isCut(from.getPort(), to.getPort())) {
            long hang = blackholeMillis;
            if (hang > 0) {
                sleep(hang);
                throw new SocketTimeoutException("Injected fault: no answer from " + to.getId());
            }
            throw new ConnectException("Injected fault: " + to.getId() + " is unreachable");
        }
        Long latency = latencyMillis.get(link(from.getPort(), to.getPort()));
        if (latency != null) {
            sleep(latency);
        }
    }

    @Override
    public void afterRequest(Node from, Node to) throws IOException {
        double rate = replyLossRate;
        if (rate > 0 && java.util.concurrent.ThreadLocalRandom.current().nextDouble() < rate) {
            throw new SocketTimeoutException("Injected fault: the reply from " + to.getId() + " was lost");
        }
    }

    @Override
    public boolean deliversClientReply(Node from) {
        double rate = clientReplyLossRate;
        return rate <= 0 || java.util.concurrent.ThreadLocalRandom.current().nextDouble() >= rate;
    }

    private static void sleep(long millis) throws IOException {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted", e);
        }
    }
}
