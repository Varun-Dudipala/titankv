package com.titankv.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Connection pool for client-side connection reuse.
 * Maintains a pool of open connections to each server node.
 */
public class ConnectionPool {

    private static final Logger logger = LoggerFactory.getLogger(ConnectionPool.class);

    private final int maxConnectionsPerHost;
    private final int connectTimeoutMs;
    private final int readTimeoutMs;
    private final Map<String, HostPool> hostPools;

    /**
     * Create a connection pool with default settings.
     */
    public ConnectionPool() {
        this(10, 5000, 30000);
    }

    /**
     * Create a connection pool with custom settings.
     *
     * @param maxConnectionsPerHost maximum connections per host
     * @param connectTimeoutMs      connection timeout in milliseconds
     * @param readTimeoutMs         read timeout in milliseconds
     */
    public ConnectionPool(int maxConnectionsPerHost, int connectTimeoutMs, int readTimeoutMs) {
        this.maxConnectionsPerHost = maxConnectionsPerHost;
        this.connectTimeoutMs = connectTimeoutMs;
        this.readTimeoutMs = readTimeoutMs;
        this.hostPools = new ConcurrentHashMap<>();
    }

    /**
     * Get a connection to the specified host.
     *
     * @param host the host address (host:port format)
     * @return a connection to the host
     * @throws IOException if connection fails
     */
    public PooledConnection acquire(String host) throws IOException {
        HostPool pool = hostPools.computeIfAbsent(host, k -> new HostPool(host));
        return pool.acquire();
    }

    /**
     * Return a connection to the pool.
     *
     * @param connection the connection to return
     */
    public void release(PooledConnection connection) {
        if (connection == null) {
            return;
        }

        HostPool pool = hostPools.get(connection.getHost());
        if (pool != null) {
            pool.release(connection);
        } else {
            connection.closeQuietly();
        }
    }

    /**
     * Close a connection and remove it from the pool.
     *
     * @param connection the connection to close
     */
    public void invalidate(PooledConnection connection) {
        if (connection == null) {
            return;
        }

        HostPool pool = hostPools.get(connection.getHost());
        if (pool != null) {
            pool.invalidate(connection);
        }
        connection.closeQuietly();
    }

    /**
     * Close all connections and clear the pool.
     */
    public void close() {
        for (HostPool pool : hostPools.values()) {
            pool.close();
        }
        hostPools.clear();
        logger.info("Connection pool closed");
    }

    /**
     * Get the total number of connections across all hosts.
     */
    public int getTotalConnections() {
        return hostPools.values().stream()
            .mapToInt(HostPool::getTotalConnections)
            .sum();
    }

    /**
     * Get the number of idle connections across all hosts.
     */
    public int getIdleConnections() {
        return hostPools.values().stream()
            .mapToInt(HostPool::getIdleConnections)
            .sum();
    }

    /**
     * Per-host connection pool.
     */
    private class HostPool {
        private final String host;
        private final String hostname;
        private final int port;
        private final Queue<PooledConnection> idle;
        private final AtomicInteger totalConnections;

        HostPool(String host) {
            this.host = host;
            String[] parts = host.split(":");
            this.hostname = parts[0];
            this.port = parts.length > 1 ? Integer.parseInt(parts[1]) : 9001;
            this.idle = new ConcurrentLinkedQueue<>();
            this.totalConnections = new AtomicInteger(0);
        }

        PooledConnection acquire() throws IOException {
            // Try to get an idle connection
            PooledConnection conn = idle.poll();
            if (conn != null && conn.isOpen()) {
                return conn;
            }

            // Create a new connection if under limit
            if (totalConnections.get() < maxConnectionsPerHost) {
                return createConnection();
            }

            // Wait for an idle connection or create one anyway
            conn = idle.poll();
            if (conn != null && conn.isOpen()) {
                return conn;
            }

            return createConnection();
        }

        private PooledConnection createConnection() throws IOException {
            SocketChannel channel = SocketChannel.open();
            channel.configureBlocking(true);
            channel.socket().setTcpNoDelay(true);
            channel.socket().setKeepAlive(true);
            channel.socket().setSoTimeout(readTimeoutMs);

            try {
                channel.socket().connect(new InetSocketAddress(hostname, port), connectTimeoutMs);
            } catch (IOException e) {
                channel.close();
                throw new IOException("Failed to connect to " + host + ": " + e.getMessage(), e);
            }

            totalConnections.incrementAndGet();
            logger.debug("Created connection to {}", host);
            return new PooledConnection(host, channel);
        }

        void release(PooledConnection conn) {
            if (conn.isOpen() && idle.size() < maxConnectionsPerHost) {
                idle.offer(conn);
            } else {
                invalidate(conn);
            }
        }

        void invalidate(PooledConnection conn) {
            totalConnections.decrementAndGet();
            conn.closeQuietly();
        }

        void close() {
            PooledConnection conn;
            while ((conn = idle.poll()) != null) {
                conn.closeQuietly();
            }
            totalConnections.set(0);
        }

        int getTotalConnections() {
            return totalConnections.get();
        }

        int getIdleConnections() {
            return idle.size();
        }
    }

    /**
     * A pooled connection wrapper.
     */
    public static class PooledConnection {
        private final String host;
        private final SocketChannel channel;
        private final ByteBuffer readBuffer;
        private final ByteBuffer writeBuffer;

        PooledConnection(String host, SocketChannel channel) {
            this.host = host;
            this.channel = channel;
            this.readBuffer = ByteBuffer.allocate(64 * 1024);
            this.writeBuffer = ByteBuffer.allocate(64 * 1024);
        }

        public String getHost() {
            return host;
        }

        public SocketChannel getChannel() {
            return channel;
        }

        public ByteBuffer getReadBuffer() {
            readBuffer.clear();
            return readBuffer;
        }

        public ByteBuffer getWriteBuffer() {
            writeBuffer.clear();
            return writeBuffer;
        }

        public boolean isOpen() {
            return channel.isOpen() && channel.isConnected();
        }

        public void write(ByteBuffer buffer) throws IOException {
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
        }

        public int read(ByteBuffer buffer) throws IOException {
            return channel.read(buffer);
        }

        void closeQuietly() {
            try {
                channel.close();
            } catch (IOException e) {
                // Ignore
            }
        }
    }
}
