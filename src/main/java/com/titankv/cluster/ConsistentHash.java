package com.titankv.cluster;

import com.titankv.util.MurmurHash3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Consistent hash ring implementation with virtual nodes.
 * Provides O(log n) key-to-node lookups and minimal key redistribution
 * when nodes are added or removed.
 */
public class ConsistentHash {

    private static final Logger logger = LoggerFactory.getLogger(ConsistentHash.class);

    /**
     * Number of virtual nodes per physical node.
     * Higher values give better distribution but use more memory.
     */
    public static final int DEFAULT_VIRTUAL_NODES = 150;

    private final int virtualNodesPerNode;

    // The hash ring: hash position -> virtual node
    private final ConcurrentSkipListMap<Long, VirtualNode> ring;

    // Track physical nodes
    private final Set<Node> physicalNodes;

    // Lock for modifications
    private final ReadWriteLock lock;

    /**
     * Create a consistent hash ring with default virtual nodes.
     */
    public ConsistentHash() {
        this(DEFAULT_VIRTUAL_NODES);
    }

    /**
     * Create a consistent hash ring with custom virtual node count.
     *
     * @param virtualNodesPerNode number of virtual nodes per physical node
     */
    public ConsistentHash(int virtualNodesPerNode) {
        this.virtualNodesPerNode = virtualNodesPerNode;
        this.ring = new ConcurrentSkipListMap<>();
        this.physicalNodes = ConcurrentHashMap.newKeySet();
        this.lock = new ReentrantReadWriteLock();
    }

    /**
     * Add a node to the hash ring.
     *
     * @param node the node to add
     */
    public void addNode(Node node) {
        lock.writeLock().lock();
        try {
            if (physicalNodes.contains(node)) {
                logger.debug("Node {} already in ring", node.getId());
                return;
            }

            physicalNodes.add(node);

            for (int i = 0; i < virtualNodesPerNode; i++) {
                String hashKey = node.getHashKey() + "#" + i;
                long hash = MurmurHash3.hash64(hashKey);
                int collisionCount = 0;
                while (ring.containsKey(hash)) {
                    collisionCount++;
                    hash = MurmurHash3.hash64(hashKey + "#c" + collisionCount);
                }
                VirtualNode vnode = new VirtualNode(node, i, hash);
                ring.put(hash, vnode);
            }

            logger.info("Added node {} with {} virtual nodes", node.getId(), virtualNodesPerNode);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Remove a node from the hash ring.
     *
     * @param node the node to remove
     */
    public void removeNode(Node node) {
        lock.writeLock().lock();
        try {
            if (!physicalNodes.remove(node)) {
                logger.debug("Node {} not in ring", node.getId());
                return;
            }

            Iterator<Map.Entry<Long, VirtualNode>> it = ring.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Long, VirtualNode> entry = it.next();
                if (entry.getValue().belongsTo(node)) {
                    it.remove();
                }
            }

            logger.info("Removed node {} from ring", node.getId());
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Get the node responsible for a given key.
     *
     * @param key the key to look up
     * @return the node responsible for this key
     * @throws IllegalStateException if no nodes are in the ring
     */
    public Node getNode(String key) {
        lock.readLock().lock();
        try {
            if (ring.isEmpty()) {
                throw new IllegalStateException("No nodes in cluster");
            }

            long hash = MurmurHash3.hash64(key);

            // Find the first node at or after this hash position
            Map.Entry<Long, VirtualNode> entry = ring.ceilingEntry(hash);

            // Wrap around if we've passed the highest hash
            if (entry == null) {
                entry = ring.firstEntry();
            }

            return entry.getValue().getPhysicalNode();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get N distinct nodes for replication.
     * Walks clockwise around the ring, collecting distinct physical nodes.
     *
     * @param key   the key to look up
     * @param count number of nodes to return
     * @return list of nodes for replication
     * @throws IllegalStateException if no nodes are in the ring
     */
    public List<Node> getNodes(String key, int count) {
        lock.readLock().lock();
        try {
            if (ring.isEmpty()) {
                throw new IllegalStateException("No nodes in cluster");
            }

            List<Node> result = new ArrayList<>();
            Set<Node> seen = new HashSet<>();

            long hash = MurmurHash3.hash64(key);
            SortedMap<Long, VirtualNode> tailMap = ring.tailMap(hash);

            // Walk from hash position to end of ring
            for (VirtualNode vnode : tailMap.values()) {
                Node node = vnode.getPhysicalNode();
                if (!seen.contains(node) && node.isAvailable()) {
                    seen.add(node);
                    result.add(node);
                    if (result.size() >= count) {
                        return result;
                    }
                }
            }

            // Wrap around from beginning of ring
            for (VirtualNode vnode : ring.values()) {
                Node node = vnode.getPhysicalNode();
                if (!seen.contains(node) && node.isAvailable()) {
                    seen.add(node);
                    result.add(node);
                    if (result.size() >= count) {
                        return result;
                    }
                }
            }

            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get all keys that would be assigned to a given node.
     * Useful for data migration when nodes are added/removed.
     *
     * @param node the node to check
     * @param keys the keys to check
     * @return keys that hash to this node
     */
    public Set<String> getKeysForNode(Node node, Set<String> keys) {
        Set<String> result = new HashSet<>();
        for (String key : keys) {
            if (getNode(key).equals(node)) {
                result.add(key);
            }
        }
        return result;
    }

    /**
     * Get the number of physical nodes in the ring.
     */
    public int getNodeCount() {
        return physicalNodes.size();
    }

    /**
     * Get all physical nodes in the ring.
     */
    public Set<Node> getAllNodes() {
        lock.readLock().lock();
        try {
            return new HashSet<>(physicalNodes);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get the number of virtual nodes in the ring.
     */
    public int getVirtualNodeCount() {
        return ring.size();
    }

    /**
     * Check if a node is in the ring.
     */
    public boolean containsNode(Node node) {
        return physicalNodes.contains(node);
    }

    /**
     * Check if the ring is empty.
     */
    public boolean isEmpty() {
        return ring.isEmpty();
    }

    /**
     * Clear all nodes from the ring.
     */
    public void clear() {
        lock.writeLock().lock();
        try {
            ring.clear();
            physicalNodes.clear();
            logger.info("Cleared hash ring");
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Calculate the distribution of keys across nodes.
     * Returns a map of node ID to the percentage of the ring it owns.
     */
    public Map<String, Double> getDistribution() {
        lock.readLock().lock();
        try {
            Map<String, Integer> counts = new HashMap<>();
            for (Node node : physicalNodes) {
                counts.put(node.getId(), 0);
            }

            for (VirtualNode vnode : ring.values()) {
                String id = vnode.getPhysicalNode().getId();
                counts.merge(id, 1, Integer::sum);
            }

            Map<String, Double> distribution = new HashMap<>();
            int total = ring.size();
            for (Map.Entry<String, Integer> entry : counts.entrySet()) {
                distribution.put(entry.getKey(),
                    total > 0 ? (entry.getValue() * 100.0) / total : 0.0);
            }

            return distribution;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get statistics about the hash ring.
     */
    public String getStats() {
        Map<String, Double> dist = getDistribution();
        StringBuilder sb = new StringBuilder();
        sb.append("ConsistentHash Stats:\n");
        sb.append("  Physical nodes: ").append(getNodeCount()).append("\n");
        sb.append("  Virtual nodes: ").append(getVirtualNodeCount()).append("\n");
        sb.append("  Distribution:\n");
        for (Map.Entry<String, Double> entry : dist.entrySet()) {
            sb.append("    ").append(entry.getKey())
              .append(": ").append(String.format("%.2f%%", entry.getValue()))
              .append("\n");
        }
        return sb.toString();
    }
}
