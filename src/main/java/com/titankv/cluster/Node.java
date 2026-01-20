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
    private final String hashHost;
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
        this.hashHost = canonicalizeHost(host);
        this.status = Status.JOINING;
        this.lastHeartbeat = System.currentTimeMillis();
    }

    /**
     * Create a node from an address string.
     * Supports IPv4 (host:port), IPv6 ([::1]:port), and hostnames.
     *
     * @param address the address in host:port or [ipv6]:port format
     * @return a new Node
     * @throws IllegalArgumentException if the address format is invalid
     */
    public static Node fromAddress(String address) {
        String host;
        int port = 9001;  // Default port

        // Handle IPv6 with brackets: [::1]:9001 or [::1]
        if (address.startsWith("[")) {
            int closeBracket = address.indexOf(']');
            if (closeBracket == -1) {
                throw new IllegalArgumentException("Invalid IPv6 address (missing ']'): " + address);
            }

            // Extract host from brackets
            host = address.substring(1, closeBracket);

            // Check for port after brackets
            if (closeBracket + 1 < address.length()) {
                if (address.charAt(closeBracket + 1) != ':') {
                    throw new IllegalArgumentException("Expected ':' after ']' in: " + address);
                }
                try {
                    port = Integer.parseInt(address.substring(closeBracket + 2));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid port number in: " + address, e);
                }
            }
        }
        // Handle IPv4 or hostname with port, or IPv6 without brackets
        else {
            int lastColon = address.lastIndexOf(':');
            int firstColon = address.indexOf(':');

            // Multiple colons = IPv6 without brackets (no port)
            if (firstColon != lastColon) {
                host = address;
            }
            // Single colon = host:port format
            else if (lastColon != -1) {
                host = address.substring(0, lastColon);
                try {
                    port = Integer.parseInt(address.substring(lastColon + 1));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid port number in: " + address, e);
                }
            }
            // No colon = just hostname (use default port)
            else {
                host = address;
            }
        }

        // Generate canonical ID with proper IPv6 bracketing
        String id = formatAddress(host, port);
        return new Node(id, host, port);
    }

    public String getId() {
        return id;
    }

    public String getHost() {
        return host;
    }

    /**
     * Get the canonical host used for hashing and identity on the ring.
     * This may differ from the advertised host if it resolves to an IP.
     */
    public String getHashHost() {
        return hashHost;
    }

    public int getPort() {
        return port;
    }

    /**
     * Get the address string for this node.
     * Returns properly formatted address with IPv6 bracketing when needed.
     *
     * @return address in host:port or [ipv6]:port format
     */
    public String getAddress() {
        return formatAddress(host, port);
    }

    /**
     * Get the canonical address string used for hashing on the ring.
     */
    public String getHashKey() {
        return formatAddress(hashHost, port);
    }

    /**
     * Format a host and port into a canonical address string.
     * IPv6 addresses are bracketed: [::1]:9001
     * IPv4 and hostnames are not: 127.0.0.1:9001, localhost:9001
     *
     * @param host the hostname or IP address
     * @param port the port number
     * @return formatted address string
     */
    private static String formatAddress(String host, int port) {
        // IPv6 addresses contain colons and need bracketing
        if (host.contains(":")) {
            return "[" + host + "]:" + port;
        }
        return host + ":" + port;
    }

    private static String canonicalizeHost(String host) {
        if (host == null || host.isEmpty()) {
            return host;
        }
        try {
            return java.net.InetAddress.getByName(host).getHostAddress();
        } catch (Exception e) {
            return host;
        }
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
