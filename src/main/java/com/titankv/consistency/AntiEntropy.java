package com.titankv.consistency;

import com.titankv.cluster.ClusterManager;
import com.titankv.cluster.Node;
import com.titankv.core.InMemoryStore;
import com.titankv.core.KeyValuePair;
import com.titankv.util.MurmurHash3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Anti-entropy repair with Merkle trees, as in Dynamo: brings two replicas back in sync for every
 * key they share, including keys nobody reads (which read repair never sees).
 *
 * For a pair of nodes, each builds a Merkle tree over the keys both of them replicate. Keys are
 * assigned to one of {@value #LEAVES} leaves by the top bits of their hash; a leaf's hash combines
 * the (key, timestamp, expiry, value) digests of its entries, and each parent hashes its two
 * children. Repair then:
 * <ol>
 *   <li>fetches the peer's root hash (8 bytes) and stops if it matches the local one;</li>
 *   <li>otherwise fetches the peer's whole tree and walks both trees top-down to the leaves that differ;</li>
 *   <li>for each differing leaf, fetches the peer's (key, version) digests and copies whichever side
 *       holds the newer version of each key to the other side.</li>
 * </ol>
 * Identical replicas cost one round trip; replicas that differ in a few keys transfer only those keys.
 */
public final class AntiEntropy {

    private static final Logger logger = LoggerFactory.getLogger(AntiEntropy.class);

    static final int DEPTH = 10;
    static final int LEAVES = 1 << DEPTH;
    private static final int TREE_SIZE = 2 * LEAVES - 1; // heap layout: node i has children 2i+1, 2i+2
    static final byte MODE_ROOT = 0;
    static final byte MODE_FULL = 1;

    private final ClusterManager clusterManager;
    private final InMemoryStore store;
    private final int replicationFactor;
    private final ReplicaIO replicaIO;
    private final PeerTransport transport;
    private final ScheduledExecutorService scheduler;

    /**
     * Fetches another node's tree and leaf digests.
     */
    public interface PeerTransport {
        byte[] merkleTree(Node peer, byte mode) throws IOException;

        byte[] merkleLeaf(Node peer, int leaf) throws IOException;
    }

    /**
     * Outcome of repairing with one peer.
     */
    public record RepairStats(int differingLeaves, int pulled, int pushed) {
    }

    /**
     * @param intervalMs how often to repair with a random live peer; 0 disables periodic repair
     */
    public AntiEntropy(ClusterManager clusterManager, InMemoryStore store, int replicationFactor,
            ReplicaIO replicaIO, PeerTransport transport, long intervalMs) {
        this.clusterManager = clusterManager;
        this.store = store;
        this.replicationFactor = replicationFactor;
        this.replicaIO = replicaIO;
        this.transport = transport;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "anti-entropy");
            t.setDaemon(true);
            return t;
        });
        if (intervalMs > 0) {
            scheduler.scheduleWithFixedDelay(this::repairWithRandomPeer, intervalMs, intervalMs,
                    TimeUnit.MILLISECONDS);
        }
    }

    private void repairWithRandomPeer() {
        try {
            List<Node> peers = new ArrayList<>();
            for (Node node : clusterManager.getAllNodes()) {
                if (!node.equals(clusterManager.getLocalNode()) && node.isAvailable()) {
                    peers.add(node);
                }
            }
            if (!peers.isEmpty()) {
                repairWith(peers.get(ThreadLocalRandom.current().nextInt(peers.size())));
            }
        } catch (IOException | RuntimeException e) {
            logger.warn("Anti-entropy round failed: {}", e.getMessage());
        }
    }

    /**
     * Synchronize every key this node and the peer both replicate.
     */
    public RepairStats repairWith(Node peer) throws IOException {
        long[] local = buildTree(peer.getId());
        long peerRoot = ByteBuffer.wrap(transport.merkleTree(peer, MODE_ROOT)).getLong();
        if (peerRoot == local[0]) {
            return new RepairStats(0, 0, 0);
        }
        long[] remote = decodeTree(transport.merkleTree(peer, MODE_FULL));
        List<Integer> leaves = new ArrayList<>();
        collectDifferingLeaves(local, remote, 0, leaves);

        int pulled = 0;
        int pushed = 0;
        for (int leaf : leaves) {
            Map<String, Digest> theirs = decodeLeaf(transport.merkleLeaf(peer, leaf));
            Map<String, Digest> ours = leafDigests(peer.getId(), leaf);
            Set<String> keys = new HashSet<>(theirs.keySet());
            keys.addAll(ours.keySet());
            for (String key : keys) {
                Digest mine = ours.get(key);
                Digest other = theirs.get(key);
                if (other != null && (mine == null || other.timestamp > mine.timestamp)) {
                    Optional<ReplicationManager.ReadResult> fresh = replicaIO.read(peer, key);
                    if (fresh.isPresent()) {
                        ReplicationManager.ReadResult r = fresh.get();
                        store.putIfNewer(key, r.getValue(), r.getTimestamp(), r.getExpiresAt());
                        pulled++;
                    }
                } else if (mine != null && (other == null || mine.timestamp > other.timestamp)) {
                    Optional<KeyValuePair> entry = store.getRaw(key);
                    if (entry.isPresent()) {
                        KeyValuePair kv = entry.get();
                        replicaIO.write(peer, key, kv.getValueUnsafe(), kv.getTimestamp(), kv.getExpiresAt());
                        pushed++;
                    }
                }
            }
        }
        if (pulled + pushed > 0) {
            logger.info("Anti-entropy with {}: {} differing leaves, pulled {} keys, pushed {} keys",
                    peer.getId(), leaves.size(), pulled, pushed);
        }
        return new RepairStats(leaves.size(), pulled, pushed);
    }

    private static void collectDifferingLeaves(long[] a, long[] b, int node, List<Integer> out) {
        if (a[node] == b[node]) {
            return;
        }
        int firstLeaf = LEAVES - 1;
        if (node >= firstLeaf) {
            out.add(node - firstLeaf);
            return;
        }
        collectDifferingLeaves(a, b, 2 * node + 1, out);
        collectDifferingLeaves(a, b, 2 * node + 2, out);
    }

    // ==================== Serving peers ====================

    /**
     * Answer a peer's MERKLE_TREE request: the root hash only, or the whole tree.
     */
    public byte[] handleTreeRequest(String peerId, byte mode) {
        long[] tree = buildTree(peerId);
        if (mode == MODE_ROOT) {
            return ByteBuffer.allocate(8).putLong(tree[0]).array();
        }
        ByteBuffer buffer = ByteBuffer.allocate(TREE_SIZE * 8);
        for (long hash : tree) {
            buffer.putLong(hash);
        }
        return buffer.array();
    }

    /**
     * Answer a peer's MERKLE_LEAF request: (key, timestamp, digest) for every shared key in the leaf.
     */
    public byte[] handleLeafRequest(String peerId, int leaf) {
        if (leaf < 0 || leaf >= LEAVES) {
            throw new IllegalArgumentException("Invalid leaf " + leaf);
        }
        Map<String, Digest> digests = leafDigests(peerId, leaf);
        int size = 4;
        List<byte[]> keys = new ArrayList<>();
        for (String key : digests.keySet()) {
            byte[] k = key.getBytes(StandardCharsets.UTF_8);
            keys.add(k);
            size += 4 + k.length + 8 + 8;
        }
        ByteBuffer buffer = ByteBuffer.allocate(size).putInt(digests.size());
        int i = 0;
        for (Digest digest : digests.values()) {
            byte[] k = keys.get(i++);
            buffer.putInt(k.length).put(k).putLong(digest.timestamp).putLong(digest.hash);
        }
        return buffer.array();
    }

    // ==================== Trees ====================

    private record Digest(long timestamp, long hash) {
    }

    /**
     * Merkle tree over the keys the local node shares with the peer, as a heap-ordered array.
     */
    long[] buildTree(String peerId) {
        long[] tree = new long[TREE_SIZE];
        int firstLeaf = LEAVES - 1;
        store.forEachEntry((key, kv) -> {
            if (sharedWith(peerId, key)) {
                tree[firstLeaf + leafOf(key)] ^= entryHash(key, kv);
            }
        });
        for (int node = firstLeaf - 1; node >= 0; node--) {
            tree[node] = combine(tree[2 * node + 1], tree[2 * node + 2]);
        }
        return tree;
    }

    private Map<String, Digest> leafDigests(String peerId, int leaf) {
        Map<String, Digest> digests = new HashMap<>();
        store.forEachEntry((key, kv) -> {
            if (leafOf(key) == leaf && sharedWith(peerId, key)) {
                digests.put(key, new Digest(kv.getTimestamp(), entryHash(key, kv)));
            }
        });
        return digests;
    }

    private boolean sharedWith(String peerId, String key) {
        List<Node> replicas = clusterManager.getReplicasForKey(key, replicationFactor);
        boolean peer = false;
        boolean self = false;
        String localId = clusterManager.getLocalNode().getId();
        for (Node replica : replicas) {
            peer |= replica.getId().equals(peerId);
            self |= replica.getId().equals(localId);
        }
        return peer && self;
    }

    static int leafOf(String key) {
        return (int) (MurmurHash3.hash64(key) >>> (64 - DEPTH));
    }

    private static long entryHash(String key, KeyValuePair kv) {
        byte[] value = kv.getValueUnsafe();
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(keyBytes.length + 8 + 8 + 8 + 1);
        buffer.put(keyBytes).putLong(kv.getTimestamp()).putLong(kv.getExpiresAt())
                .putLong(value != null ? MurmurHash3.hash64(value) : 0).put((byte) (value == null ? 1 : 0));
        return MurmurHash3.hash64(buffer.array());
    }

    private static long combine(long left, long right) {
        return MurmurHash3.hash64(ByteBuffer.allocate(16).putLong(left).putLong(right).array());
    }

    private static long[] decodeTree(byte[] bytes) throws IOException {
        if (bytes.length != TREE_SIZE * 8) {
            throw new IOException("Unexpected Merkle tree size " + bytes.length);
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        long[] tree = new long[TREE_SIZE];
        for (int i = 0; i < TREE_SIZE; i++) {
            tree[i] = buffer.getLong();
        }
        return tree;
    }

    private static Map<String, Digest> decodeLeaf(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int count = buffer.getInt();
        Map<String, Digest> digests = new HashMap<>();
        for (int i = 0; i < count; i++) {
            byte[] key = new byte[buffer.getInt()];
            buffer.get(key);
            digests.put(new String(key, StandardCharsets.UTF_8), new Digest(buffer.getLong(), buffer.getLong()));
        }
        return digests;
    }

    public void shutdown() {
        scheduler.shutdownNow();
    }
}
