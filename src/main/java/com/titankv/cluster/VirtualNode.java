package com.titankv.cluster;

import java.util.Objects;

/**
 * Represents a virtual node in the consistent hash ring.
 * Multiple virtual nodes map to a single physical node for better distribution.
 */
public class VirtualNode {

    private final Node physicalNode;
    private final int replicaIndex;
    private final long hashValue;

    /**
     * Create a virtual node.
     *
     * @param physicalNode the physical node this virtual node represents
     * @param replicaIndex the index of this virtual node (0 to N-1)
     * @param hashValue    the hash position on the ring
     */
    public VirtualNode(Node physicalNode, int replicaIndex, long hashValue) {
        this.physicalNode = physicalNode;
        this.replicaIndex = replicaIndex;
        this.hashValue = hashValue;
    }

    public Node getPhysicalNode() {
        return physicalNode;
    }

    public int getReplicaIndex() {
        return replicaIndex;
    }

    public long getHashValue() {
        return hashValue;
    }

    /**
     * Get the key used for hashing this virtual node.
     *
     * @return the hash key string
     */
    public String getHashKey() {
        return physicalNode.getHashKey() + "#" + replicaIndex;
    }

    /**
     * Check if this virtual node belongs to the given physical node.
     */
    public boolean belongsTo(Node node) {
        return physicalNode.equals(node);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VirtualNode that = (VirtualNode) o;
        return replicaIndex == that.replicaIndex &&
               Objects.equals(physicalNode, that.physicalNode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(physicalNode, replicaIndex);
    }

    @Override
    public String toString() {
        return "VirtualNode{" +
               "physicalNode=" + physicalNode.getId() +
               ", replicaIndex=" + replicaIndex +
               ", hashValue=" + hashValue +
               '}';
    }
}
