package com.titankv.cluster;

import java.util.Objects;

/**
 * Represents a physical node in the TitanKV cluster.
 */
public class Node {

    /**
     * Node status enumeration.
     */
    public enum Status {
        ALIVE,      // Node is healthy and responding
        SUSPECT,    // Node may be failing (missed heartbeats)
        DEAD,       // Node is confirmed dead
        LEAVING,    // Node is gracefully leaving the cluster
        JOINING     // Node is in the process of joining
    }

    private final String id;
    private final String host;
    private final int port;
    private volatile Status status;
    private volatile long lastHeartbeat;

    /**
     * Create a new node.
     *
     * @param id   unique node identifier
     * @param host hostname or IP address
     * @param port port number
     */
    public Node(String id, String host, int port) {
        this.id = id;
        this.host = host;
        this.port = port;
        this.status = Status.JOINING;
        this.lastHeartbeat = System.currentTimeMillis();
    }

    /**
     * Create a node from an address string (host:port).
     *
     * @param address the address in host:port format
     * @return a new Node
     */
    public static Node fromAddress(String address) {
        String[] parts = address.split(":");
        String host = parts[0];
        int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 9001;
        String id = host + ":" + port;
        return new Node(id, host, port);
    }

    public String getId() {
        return id;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    /**
     * Get the address string for this node.
     *
     * @return address in host:port format
     */
    public String getAddress() {
        return host + ":" + port;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public long getLastHeartbeat() {
        return lastHeartbeat;
    }

    public void updateHeartbeat() {
        this.lastHeartbeat = System.currentTimeMillis();
        if (this.status == Status.SUSPECT) {
            this.status = Status.ALIVE;
        }
    }

    /**
     * Check if the node is available for requests.
     */
    public boolean isAvailable() {
        return status == Status.ALIVE || status == Status.JOINING;
    }

    /**
     * Get milliseconds since last heartbeat.
     */
    public long getMillisSinceLastHeartbeat() {
        return System.currentTimeMillis() - lastHeartbeat;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Node node = (Node) o;
        return port == node.port &&
               Objects.equals(id, node.id) &&
               Objects.equals(host, node.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, host, port);
    }

    @Override
    public String toString() {
        return "Node{" +
               "id='" + id + '\'' +
               ", address=" + getAddress() +
               ", status=" + status +
               '}';
    }
}
