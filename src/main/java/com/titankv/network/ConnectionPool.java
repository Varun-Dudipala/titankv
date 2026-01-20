package com.titankv.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.titankv.network.protocol.BinaryProtocol;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Connection pool for client-side connection reuse.
 * Maintains a pool of open connections to each server node.
 */
public class ConnectionPool {

    private static final Logger logger = LoggerFactory.getLogger(ConnectionPool.class);

    // Shared timeout executor for enforcing read timeouts
    private static final ScheduledExecutorService TIMEOUT_EXECUTOR = Executors.newScheduledThreadPool(2, r -> {
        Thread t = new Thread(r, "connection-timeout");
        t.setDaemon(true);
        return t;
    });

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
     * Parse host:port string with IPv6 support.
     *
     * Formats supported:
     * - IPv6 with brackets and port: [::1]:9001, [fe80::1]:9001
     * - IPv6 with brackets, no port: [::1] (uses defaultPort)
     * - IPv6 without brackets: ::1, fe80::1 (uses defaultPort)
     * - IPv4 with port: 127.0.0.1:9001
     * - IPv4 without port: 127.0.0.1 (uses defaultPort)
     * - Hostname with port: localhost:9001
     * - Hostname without port: localhost (uses defaultPort)
     *
     * @param hostPort    the host:port string
     * @param defaultPort the default port if not specified
     * @return parsed hostname and port
     */
    private static HostPort parseHostPort(String hostPort, int defaultPort) {
        if (hostPort == null || hostPort.isEmpty()) {
            throw new IllegalArgumentException("Host cannot be null or empty");
        }

        String hostname;
        int port = defaultPort;

        // Handle IPv6 with brackets: [::1]:9001 or [::1]
        if (hostPort.startsWith("[")) {
            int closeBracket = hostPort.indexOf(']');
            if (closeBracket == -1) {
                throw new IllegalArgumentException("Invalid IPv6 address (missing ']'): " + hostPort);
            }

            // Extract hostname from brackets
            hostname = hostPort.substring(1, closeBracket);

            // Check for port after brackets
            if (closeBracket + 1 < hostPort.length()) {
                if (hostPort.charAt(closeBracket + 1) != ':') {
                    throw new IllegalArgumentException("Expected ':' after ']' in: " + hostPort);
                }
                try {
                    port = Integer.parseInt(hostPort.substring(closeBracket + 2));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid port number in: " + hostPort, e);
                }
            }
        }
        // Handle IPv4 or hostname with port, or IPv6 without brackets
        else {
            int lastColon = hostPort.lastIndexOf(':');
            int firstColon = hostPort.indexOf(':');

            // Multiple colons = IPv6 without brackets (no port)
            if (firstColon != lastColon) {
                hostname = hostPort;
            }
            // Single colon = host:port format
            else if (lastColon != -1) {
                hostname = hostPort.substring(0, lastColon);
                try {
                    port = Integer.parseInt(hostPort.substring(lastColon + 1));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid port number in: " + hostPort, e);
                }
            }
            // No colon = just hostname (use default port)
            else {
                hostname = hostPort;
            }
        }

        return new HostPort(hostname, port);
    }

    /**
     * Holder for parsed hostname and port.
     */
    private static class HostPort {
        final String hostname;
        final int port;

        HostPort(String hostname, int port) {
            this.hostname = hostname;
            this.port = port;
        }
    }

    /**
     * Per-host connection pool with semaphore-based limit enforcement.
     */
    private class HostPool {
        private final String host;
        private final String hostname;
        private final int port;
        private final Queue<PooledConnection> idle;
        private final Semaphore permits;
        private final AtomicInteger totalConnections;
        private final java.util.Set<PooledConnection> allConnections;
        private volatile boolean closed = false;

        HostPool(String host) {
            this.host = host;
            HostPort parsed = parseHostPort(host, 9001);
            this.hostname = parsed.hostname;
            this.port = parsed.port;
            this.idle = new ConcurrentLinkedQueue<>();
            this.permits = new Semaphore(maxConnectionsPerHost);
            this.totalConnections = new AtomicInteger(0);
            this.allConnections = ConcurrentHashMap.newKeySet();
        }

        PooledConnection acquire() throws IOException {
            if (closed) {
                throw new IOException("Connection pool is closed for " + host);
            }
            // Try to get idle connection without blocking
            PooledConnection conn = idle.poll();
            if (conn != null && conn.isOpen()) {
                return conn;
            }
            // Connection from pool was stale, release its permit
            if (conn != null) {
                permits.release();
                totalConnections.decrementAndGet();
            }

            // Try to acquire permit (with timeout)
            boolean acquired;
            try {
                acquired = permits.tryAcquire(connectTimeoutMs, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted waiting for connection", e);
            }

            if (!acquired) {
                throw new IOException("Connection pool exhausted for " + host);
            }

            try {
                return createConnection();
            } catch (IOException e) {
                permits.release();
                throw e;
            }
        }

        private PooledConnection createConnection() throws IOException {
            SocketChannel channel = SocketChannel.open();

            // Set socket options before connecting
            channel.socket().setTcpNoDelay(true);
            channel.socket().setKeepAlive(true);

            // Set read timeout (only works for blocking mode)
            // NOTE: This timeout applies to Socket.getInputStream().read() but NOT to
            // SocketChannel.read(). For true timeout enforcement with SocketChannel,
            // use selector-based non-blocking I/O or SO_TIMEOUT before configuring
            // blocking.
            channel.socket().setSoTimeout(readTimeoutMs);

            // Configure as blocking for simpler client implementation
            channel.configureBlocking(true);

            try {
                channel.socket().connect(new InetSocketAddress(hostname, port), connectTimeoutMs);
            } catch (IOException e) {
                channel.close();
                throw new IOException("Failed to connect to " + host + ": " + e.getMessage(), e);
            }

            totalConnections.incrementAndGet();
            logger.debug("Created connection to {}", host);
            PooledConnection connection = new PooledConnection(host, channel);
            allConnections.add(connection);
            return connection;
        }

        void release(PooledConnection conn) {
            if (closed) {
                invalidate(conn);
                return;
            }
            synchronized (idle) {
                if (conn.isOpen() && idle.size() < maxConnectionsPerHost) {
                    idle.offer(conn);
                } else {
                    invalidate(conn);
                }
            }
        }

        void invalidate(PooledConnection conn) {
            conn.closeQuietly();
            totalConnections.decrementAndGet();
            allConnections.remove(conn);
            permits.release();
        }

        void close() {
            closed = true;
            for (PooledConnection conn : allConnections) {
                conn.closeQuietly();
            }
            allConnections.clear();
            idle.clear();
            permits.drainPermits();
            permits.release(maxConnectionsPerHost);
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
     * A pooled connection wrapper with dynamic buffer support.
     */
    public static class PooledConnection {
        private static final int INITIAL_BUFFER_SIZE = 64 * 1024;
        private static final int MAX_READ_BUFFER_SIZE = BinaryProtocol.RESPONSE_HEADER_SIZE
                + BinaryProtocol.MAX_VALUE_LENGTH;
        private static final int MAX_WRITE_BUFFER_SIZE = BinaryProtocol.REQUEST_HEADER_SIZE
                + BinaryProtocol.MAX_KEY_LENGTH + BinaryProtocol.MAX_VALUE_LENGTH;

        private final String host;
        private final SocketChannel channel;
        private ByteBuffer readBuffer; // Mutable for dynamic growth
        private ByteBuffer writeBuffer; // Mutable for dynamic growth
        private boolean authenticated;

        PooledConnection(String host, SocketChannel channel) {
            this.host = host;
            this.channel = channel;
            this.readBuffer = ByteBuffer.allocate(INITIAL_BUFFER_SIZE);
            this.writeBuffer = ByteBuffer.allocate(INITIAL_BUFFER_SIZE);
            this.authenticated = false;
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

        /**
         * Return the read buffer without clearing it.
         * Used during incremental reads to preserve partial data.
         */
        public ByteBuffer peekReadBuffer() {
            return readBuffer;
        }

        public ByteBuffer getWriteBuffer() {
            writeBuffer.clear();
            return writeBuffer;
        }

        public boolean isAuthenticated() {
            return authenticated;
        }

        public void markAuthenticated() {
            this.authenticated = true;
        }

        /**
         * Grow read buffer to accommodate larger responses.
         * PRECONDITION: Buffer must be in WRITE MODE (after compact or initial state).
         * POSTCONDITION: Buffer is in WRITE MODE with all unread bytes preserved at the
         * start.
         */
        public void growReadBuffer() {
            int currentCapacity = readBuffer.capacity();
            if (currentCapacity >= MAX_READ_BUFFER_SIZE) {
                throw new IllegalStateException("Read buffer at maximum size " + currentCapacity +
                        ", response too large for " + host);
            }

            // Current buffer is in write mode: position=end of data, limit=capacity
            // We need to preserve bytes from [0, position)
            int preservedBytes = readBuffer.position();

            // Double the size, but cap at MAX_READ_BUFFER_SIZE
            long newCapacity = Math.min((long) currentCapacity * 2, MAX_READ_BUFFER_SIZE);
            logger.debug("Growing read buffer from {} to {} bytes for {} (preserving {} bytes)",
                    currentCapacity, newCapacity, host, preservedBytes);

            ByteBuffer newBuffer = ByteBuffer.allocate((int) newCapacity);

            // Copy preserved bytes: flip to read mode, copy, then back to write mode
            readBuffer.flip();
            newBuffer.put(readBuffer);

            // newBuffer is now in write mode with position at end of copied data
            readBuffer = newBuffer;
        }

        /**
         * Grow write buffer to accommodate larger requests.
         * Called when encoding requires more space.
         */
        public void growWriteBuffer() {
            int currentCapacity = writeBuffer.capacity();
            if (currentCapacity >= MAX_WRITE_BUFFER_SIZE) {
                throw new IllegalStateException("Write buffer at maximum size " + currentCapacity);
            }

            long newCapacity = Math.min((long) currentCapacity * 2, MAX_WRITE_BUFFER_SIZE);
            logger.debug("Growing write buffer from {} to {} bytes for {}",
                    currentCapacity, newCapacity, host);

            ByteBuffer newBuffer = ByteBuffer.allocate((int) newCapacity);
            writeBuffer.flip();
            newBuffer.put(writeBuffer);
            writeBuffer = newBuffer;
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

        /**
         * Read from the channel with enforced timeout.
         * If the read takes longer than timeoutMs, the channel is closed and
         * SocketTimeoutException is thrown.
         *
         * @param buffer    the buffer to read into
         * @param timeoutMs timeout in milliseconds
         * @return number of bytes read, or -1 if end of stream
         * @throws SocketTimeoutException if the read times out
         * @throws IOException            if an I/O error occurs
         */
        public int readWithTimeout(ByteBuffer buffer, long timeoutMs) throws IOException {
            if (timeoutMs <= 0) {
                // No timeout enforcement
                return channel.read(buffer);
            }

            // Track if timeout fired
            AtomicReference<ScheduledFuture<?>> timeoutTask = new AtomicReference<>();
            AtomicReference<IOException> timeoutException = new AtomicReference<>();

            try {
                // Schedule timeout: if it fires, close the channel
                ScheduledFuture<?> task = TIMEOUT_EXECUTOR.schedule(() -> {
                    try {
                        logger.debug("Read timeout after {}ms, closing connection to {}", timeoutMs, host);
                        channel.close();
                        timeoutException.set(new SocketTimeoutException(
                                "Read timeout after " + timeoutMs + "ms for " + host));
                    } catch (IOException e) {
                        logger.debug("Error closing channel on timeout: {}", e.getMessage());
                    }
                }, timeoutMs, TimeUnit.MILLISECONDS);

                timeoutTask.set(task);

                // Perform blocking read
                int bytesRead = channel.read(buffer);

                // Read completed successfully, cancel timeout
                task.cancel(false);

                // Check if timeout fired before we could cancel
                IOException timedOut = timeoutException.get();
                if (timedOut != null) {
                    throw timedOut;
                }

                return bytesRead;

            } catch (IOException e) {
                // Cancel timeout if still pending
                ScheduledFuture<?> task = timeoutTask.get();
                if (task != null) {
                    task.cancel(false);
                }

                // Check if this was a timeout-induced close
                IOException timedOut = timeoutException.get();
                if (timedOut != null) {
                    throw timedOut;
                }

                throw e;
            }
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
