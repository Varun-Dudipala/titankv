package com.titankv.util;

import com.titankv.cluster.ClusterManager;
import com.titankv.cluster.ConsistentHash;
import com.titankv.cluster.Node;
import com.titankv.cluster.VirtualNode;
import com.titankv.consistency.HintedHandoff;
import com.titankv.consistency.ReplicationManager;
import com.titankv.core.InMemoryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The JSON behind the dashboard: this node's own numbers and its view of the membership, the
 * hash ring, the replicas of a key, and the whole cluster as seen from this node.
 *
 * <p>Everything here only reads. Nothing changes the store or the membership, and nothing
 * reports the cluster secret or a token.
 *
 * <p>Some numbers exist only on the node they describe: its operation counts, its pending hints,
 * whether it is ready. So that one page can show every node, the node serving the page asks each
 * of the others for {@code /api/node} over HTTP and passes the answers on. It asks from here and
 * not from the browser because the browser often cannot reach the other nodes under the names
 * the cluster uses for them (Docker service names, for one).
 */
final class DashboardData {

    private static final Logger logger = LoggerFactory.getLogger(DashboardData.class);

    /** A peer is not asked again while its last answer is younger than this. */
    private static final long PEER_REFRESH_MS = 500;
    /** How long a request for the cluster view waits for the peers it has just asked. */
    private static final long PEER_WAIT_MS = 300;
    /** An answer older than this is not shown: the peer has stopped answering. */
    private static final long PEER_STALE_MS = 3000;
    private static final int PEER_CONNECT_TIMEOUT_MS = 300;
    private static final int PEER_READ_TIMEOUT_MS = 500;
    private static final int MAX_PEER_RESPONSE_BYTES = 64 * 1024;
    private static final String NODE_JSON_PREFIX = "{\"node\":\"";

    private final MetricsCollector metrics;
    private final ClusterManager clusterManager;
    private final InMemoryStore store;
    private final ReplicationManager replicationManager;
    private final int replicationFactor;
    // Where the other nodes serve HTTP is not gossiped, so it is inferred from this node: either
    // the same distance from the client port (the default is 90, and nodes sharing a machine are
    // usually spaced alike), or the same port (nodes on separate machines given one fixed port)
    private final int httpPort;
    private final int httpPortOffset;
    private final long startedAtMs = System.currentTimeMillis();

    // Counting live keys walks the whole store. The counts are reused for at least a second
    // however many pages and peers ask, and for 20 times as long as the last count took, so a
    // large store spends at most a twentieth of one thread on it. Only one thread counts at a
    // time; the others report the previous counts instead of waiting behind it.
    private static final long KEY_COUNT_REFRESH_MS = 1000;
    private static final int KEY_COUNT_COST_FACTOR = 20;
    private final Object keyCountLock = new Object();
    private int[] keyCounts; // live keys, then all entries including tombstones and expired ones
    private long keyCountsAtNanos;
    private long keyCountsMaxAgeNanos = TimeUnit.MILLISECONDS.toNanos(KEY_COUNT_REFRESH_MS);
    private boolean countingKeys;

    private final Map<String, Peer> peers = new ConcurrentHashMap<>();
    private final ExecutorService peerFetcher = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "metrics-http-peer");
        t.setDaemon(true);
        return t;
    });

    /** The last answer from one peer, and the request to it that is under way, if any. */
    private static final class Peer {
        String json;
        long answeredAtNanos;
        boolean asked;
        long askedAtNanos;
        CompletableFuture<Void> inFlight;
        int httpPort; // the port it last answered on, 0 if it has not answered yet
    }

    DashboardData(int httpPort, MetricsCollector metrics, ClusterManager clusterManager, InMemoryStore store,
                  ReplicationManager replicationManager) {
        this.metrics = metrics;
        this.clusterManager = clusterManager;
        this.store = store;
        this.replicationManager = replicationManager;
        this.replicationFactor = replicationManager != null ? replicationManager.getReplicationFactor() : 3;
        this.httpPort = httpPort;
        this.httpPortOffset = httpPort - clusterManager.getLocalNode().getPort();
    }

    /**
     * Copies of a key in this cluster: the replication factor, or the member count if that is
     * smaller. The consistency levels are counted against this, not the configured factor.
     */
    private int copies() {
        return replicationManager != null ? replicationManager.effectiveReplicationFactor()
                : Math.max(1, Math.min(replicationFactor, clusterManager.getNodeCount()));
    }

    void close() {
        peerFetcher.shutdownNow();
    }

    // ==================== /api/node ====================

    /**
     * This node's own numbers and its view of every member.
     */
    String nodeJson() {
        JsonWriter json = new JsonWriter();
        writeLocalNode(json);
        return json.toString();
    }

    private void writeLocalNode(JsonWriter json) {
        Node local = clusterManager.getLocalNode();
        long now = System.currentTimeMillis();
        json.beginObject()
                .name("node").value(local.getId())
                .name("address").value(local.getAddress())
                .name("version").value(Version.CURRENT)
                .name("time_ms").value(now)
                .name("uptime_ms").value(now - startedAtMs)
                .name("ready").value(clusterManager.isReady())
                .name("catching_up").value(clusterManager.isCatchingUp())
                .name("healthy").value(store.isHealthy())
                .name("connections").value(metrics.getActiveConnections());

        json.name("ops").beginObject()
                .name("gets").value(metrics.getTotalGetOps())
                .name("puts").value(metrics.getTotalPutOps())
                .name("deletes").value(metrics.getTotalDeleteOps())
                .name("errors").value(metrics.getTotalErrors())
                .name("replica_reads").value(metrics.getTotalReplicaReads())
                .name("replica_writes").value(metrics.getTotalReplicaWrites())
                .endObject();

        json.name("latency_ms").beginObject()
                .name("get_mean").value(metrics.getGetMeanLatencyMs(), 3)
                .name("get_p99").value(metrics.getGetP99LatencyMs(), 3)
                .name("put_mean").value(metrics.getPutMeanLatencyMs(), 3)
                .name("put_p99").value(metrics.getPutP99LatencyMs(), 3)
                .endObject();

        HintedHandoff hints = replicationManager != null ? replicationManager.getHintedHandoff() : null;
        json.name("hints").beginObject()
                .name("pending").value(hints != null ? hints.totalPendingHints() : 0)
                .name("bytes").value(hints != null ? hints.totalPendingHintBytes() : 0)
                .name("by_target").beginObject();
        if (hints != null) {
            for (Node member : sortedMembers()) {
                int pending = hints.pendingHints(member.getId());
                if (pending > 0) {
                    json.name(member.getId()).value(pending);
                }
            }
        }
        json.endObject().endObject();

        int[] keys = keyCounts(); // one reading for both numbers
        json.name("store").beginObject()
                .name("keys").value(keys[0])
                .name("keys_raw").value(keys[1])
                .name("memory_bytes").value(store.getMemoryUsedBytes())
                .name("wal_enabled").value(store.isWalEnabled())
                .name("wal_syncs").value(store.getWalSyncCount())
                .endObject();

        json.name("members").beginArray();
        for (Node member : sortedMembers()) {
            json.beginObject();
            writeMember(json, member);
            json.endObject();
        }
        json.endArray().endObject();
    }

    private int[] keyCounts() {
        synchronized (keyCountLock) {
            while (true) {
                boolean fresh = keyCounts != null && System.nanoTime() - keyCountsAtNanos < keyCountsMaxAgeNanos;
                if (fresh || (keyCounts != null && countingKeys)) {
                    return keyCounts;
                }
                if (!countingKeys) {
                    countingKeys = true;
                    break;
                }
                try {
                    keyCountLock.wait(); // nothing to report yet: wait for the first count
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return new int[] {0, 0};
                }
            }
        }
        int[] counted = null;
        long started = System.nanoTime();
        try {
            int raw = store.rawSize();
            int live = store.size();
            // Writes land between the two counts; never report more live keys than entries
            counted = new int[] {live, Math.max(raw, live)};
            return counted;
        } finally {
            long finished = System.nanoTime();
            synchronized (keyCountLock) {
                if (counted != null) {
                    keyCounts = counted;
                    keyCountsAtNanos = finished;
                    keyCountsMaxAgeNanos = Math.max(TimeUnit.MILLISECONDS.toNanos(KEY_COUNT_REFRESH_MS),
                            KEY_COUNT_COST_FACTOR * (finished - started));
                }
                countingKeys = false;
                keyCountLock.notifyAll();
            }
        }
    }

    /** A member as this node sees it: the fields shared by every endpoint that lists members. */
    private void writeMember(JsonWriter json, Node member) {
        boolean local = member.equals(clusterManager.getLocalNode());
        json.name("id").value(member.getId())
                .name("address").value(member.getAddress())
                .name("status").value(member.getStatus().name())
                .name("up").value(member.isAvailable())
                .name("local").value(local)
                // A node does not gossip to itself, so the age of its own heartbeat means nothing
                .name("heartbeat_age_ms").value(local ? 0 : member.getMillisSinceLastHeartbeat());
    }

    private List<Node> sortedMembers() {
        List<Node> members = new ArrayList<>(clusterManager.getAllNodes());
        members.sort(Comparator.comparing(Node::getId));
        return members;
    }

    // ==================== /api/cluster ====================

    /**
     * Every member as this node sees it, each with the numbers that member reports about itself
     * ({@code "self"}, the same object as {@code /api/node}), or null if it did not answer.
     */
    String clusterJson() {
        List<Node> members = sortedMembers();
        Map<String, String> answers = askPeers(members);

        JsonWriter json = new JsonWriter();
        json.beginObject()
                .name("served_by").value(clusterManager.getLocalNode().getId())
                .name("time_ms").value(System.currentTimeMillis())
                .name("replication_factor").value(replicationFactor)
                .name("copies").value(copies())
                .name("quorum").value(copies() / 2 + 1)
                .name("nodes").beginArray();
        for (Node member : members) {
            json.beginObject();
            writeMember(json, member);
            json.name("self");
            if (member.equals(clusterManager.getLocalNode())) {
                writeLocalNode(json);
            } else {
                String answer = answers.get(member.getId());
                if (answer != null) {
                    json.raw(answer);
                } else {
                    json.value((String) null);
                }
            }
            json.endObject();
        }
        json.endArray().endObject();
        return json.toString();
    }

    /**
     * Ask every other member for its own numbers, at most once per {@value #PEER_REFRESH_MS} ms
     * each however many pages are open, and wait briefly for the answers. A peer that is slow or
     * gone costs the caller no more than {@value #PEER_WAIT_MS} ms: its request carries on in the
     * background and the caller reports what it has.
     *
     * @return the latest answer by node id, without the peers that have not answered lately
     */
    private Map<String, String> askPeers(List<Node> members) {
        long now = System.nanoTime();
        Set<String> ids = new HashSet<>();
        List<CompletableFuture<Void>> pending = new ArrayList<>();
        for (Node member : members) {
            ids.add(member.getId());
            if (member.equals(clusterManager.getLocalNode())) {
                continue;
            }
            Peer peer = peers.computeIfAbsent(member.getId(), id -> new Peer());
            synchronized (peer) {
                boolean due = !peer.asked || now - peer.askedAtNanos >= TimeUnit.MILLISECONDS.toNanos(PEER_REFRESH_MS);
                if (peer.inFlight == null && due) {
                    try {
                        peer.asked = true;
                        peer.askedAtNanos = now;
                        peer.inFlight = CompletableFuture.runAsync(() -> ask(member, peer), peerFetcher);
                    } catch (RejectedExecutionException e) {
                        // The server is stopping
                    }
                }
                if (peer.inFlight != null) {
                    pending.add(peer.inFlight);
                }
            }
        }
        peers.keySet().retainAll(ids); // members that have left the cluster

        if (!pending.isEmpty()) {
            try {
                CompletableFuture.allOf(pending.toArray(new CompletableFuture[0]))
                        .get(PEER_WAIT_MS, TimeUnit.MILLISECONDS);
            } catch (TimeoutException | ExecutionException e) {
                // Report what has arrived
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        Map<String, String> answers = new LinkedHashMap<>();
        long after = System.nanoTime();
        for (Map.Entry<String, Peer> entry : peers.entrySet()) {
            Peer peer = entry.getValue();
            synchronized (peer) {
                if (peer.json != null
                        && after - peer.answeredAtNanos < TimeUnit.MILLISECONDS.toNanos(PEER_STALE_MS)) {
                    answers.put(entry.getKey(), peer.json);
                }
            }
        }
        return answers;
    }

    private void ask(Node member, Peer peer) {
        String json = null;
        int answeredOn = 0;
        try {
            int known;
            synchronized (peer) {
                known = peer.httpPort;
            }
            for (int port : candidatePorts(known, member.getPort(), httpPortOffset, httpPort)) {
                try {
                    json = fetchNodeJson(member, port);
                } catch (IOException | RuntimeException e) {
                    logger.debug("No answer from {} on port {} for the dashboard: {}", member.getId(), port,
                            e.toString());
                }
                if (json != null) {
                    answeredOn = port;
                    break;
                }
            }
        } finally {
            // In a finally: if this were skipped (an Error, say), the peer would count as being
            // asked for ever and never be asked again
            synchronized (peer) {
                peer.json = json;
                peer.httpPort = answeredOn;
                peer.answeredAtNanos = System.nanoTime();
                peer.inFlight = null;
            }
        }
    }

    /**
     * The ports a member may serve HTTP on, most likely first: the one it last answered on, the
     * one at this node's distance from the client port, and this node's own HTTP port.
     */
    static List<Integer> candidatePorts(int lastAnsweredOn, int memberPort, int httpPortOffset, int httpPort) {
        List<Integer> ports = new ArrayList<>(3);
        for (int port : new int[] {lastAnsweredOn, memberPort + httpPortOffset, httpPort}) {
            if (port > 0 && port <= 65535 && !ports.contains(port)) {
                ports.add(port);
            }
        }
        return ports;
    }

    private String fetchNodeJson(Node member, int port) throws IOException {
        HttpURLConnection connection;
        try {
            URI uri = new URI("http", null, member.getHost(), port, "/api/node", null, null);
            connection = (HttpURLConnection) uri.toURL().openConnection(Proxy.NO_PROXY);
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
        connection.setConnectTimeout(PEER_CONNECT_TIMEOUT_MS);
        connection.setReadTimeout(PEER_READ_TIMEOUT_MS);
        connection.setUseCaches(false);
        connection.setInstanceFollowRedirects(false);
        try {
            if (connection.getResponseCode() != 200) {
                return null;
            }
            byte[] body;
            try (InputStream in = connection.getInputStream()) {
                body = in.readNBytes(MAX_PEER_RESPONSE_BYTES + 1);
            }
            if (body.length > MAX_PEER_RESPONSE_BYTES) {
                return null;
            }
            // The answer is passed on inside this node's own JSON. It must be one whole JSON
            // object, or it could end this node's structure early or leave it unparsable. What
            // the fields hold is the page's to check.
            String json = new String(body, StandardCharsets.UTF_8).trim();
            // Its own id first: the answer must come from this member. A guessed port can
            // belong to another node on the same machine, this one included.
            String expected = NODE_JSON_PREFIX + JsonWriter.escape(member.getId()) + "\"";
            return json.startsWith(expected) && JsonCheck.isObject(json) ? json : null;
        } finally {
            connection.disconnect();
        }
    }

    // ==================== /api/ring ====================

    /**
     * Every virtual node's position on the ring (0 to 1, in order) with the index of its owner
     * in {@code nodes}, and the share of the hash space each node is the first replica for.
     */
    String ringJson() {
        ConsistentHash ring = clusterManager.getHashRing();
        List<VirtualNode> vnodes = ring.getVirtualNodes();

        List<String> ids = new ArrayList<>();
        for (VirtualNode vnode : vnodes) {
            String id = vnode.getPhysicalNode().getId();
            if (!ids.contains(id)) {
                ids.add(id);
            }
        }
        ids.sort(Comparator.naturalOrder());

        double[] share = new double[ids.size()];
        int[] count = new int[ids.size()];
        int[] owner = new int[vnodes.size()];
        double[] position = new double[vnodes.size()];
        for (int i = 0; i < vnodes.size(); i++) {
            position[i] = ConsistentHash.positionOf(vnodes.get(i).getHashValue());
            owner[i] = ids.indexOf(vnodes.get(i).getPhysicalNode().getId());
        }
        for (int i = 0; i < vnodes.size(); i++) {
            // The first virtual node's range wraps round from the last one
            double start = i == 0 ? position[vnodes.size() - 1] - 1.0 : position[i - 1];
            share[owner[i]] += position[i] - start;
            count[owner[i]]++;
        }

        JsonWriter json = new JsonWriter();
        json.beginObject()
                .name("virtual_nodes").value(vnodes.size())
                .name("nodes").beginArray();
        for (int i = 0; i < ids.size(); i++) {
            json.beginObject()
                    .name("id").value(ids.get(i))
                    .name("virtual_nodes").value(count[i])
                    .name("share").value(share[i], 4)
                    .endObject();
        }
        json.endArray().name("vnodes").beginArray();
        for (int i = 0; i < vnodes.size(); i++) {
            json.beginArray().value(position[i], 6).value(owner[i]).endArray();
        }
        json.endArray().endObject();
        return json.toString();
    }

    // ==================== /api/replicas ====================

    /**
     * The nodes that hold a key, in ring order, and whether this node considers each of them up.
     * They are the key's replicas whether or not they are up: the set only changes when a node
     * joins or leaves.
     */
    String replicasJson(String key) {
        List<Node> replicas;
        try {
            replicas = clusterManager.getReplicasForKey(key, replicationFactor);
        } catch (IllegalStateException e) {
            replicas = List.of(); // an empty ring
        }
        long hash = MurmurHash3.hash64(key);
        // What QUORUM and ALL are counted against: a cluster smaller than the replication
        // factor keeps fewer copies and needs fewer acknowledgements
        int copies = copies();
        int up = 0;
        for (Node replica : replicas) {
            if (replica.isAvailable()) {
                up++;
            }
        }

        JsonWriter json = new JsonWriter();
        json.beginObject()
                .name("key").value(key)
                .name("hash").value(Long.toString(hash)) // a string: JavaScript numbers hold only 53 bits
                .name("position").value(ConsistentHash.positionOf(hash), 6)
                .name("replication_factor").value(replicationFactor)
                .name("copies").value(copies)
                .name("quorum").value(copies / 2 + 1)
                .name("replicas_up").value(up)
                .name("replicas").beginArray();
        for (Node replica : replicas) {
            json.beginObject();
            writeMember(json, replica);
            json.endObject();
        }
        json.endArray().endObject();
        return json.toString();
    }
}
