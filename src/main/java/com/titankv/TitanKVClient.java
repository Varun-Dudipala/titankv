package com.titankv;

import com.titankv.client.ClientConfig;
import com.titankv.cluster.ConsistentHash;
import com.titankv.cluster.Node;
import com.titankv.network.ConnectionPool;
import com.titankv.network.ConnectionPool.PooledConnection;
import com.titankv.network.protocol.BinaryProtocol;
import com.titankv.network.protocol.Command;
import com.titankv.network.protocol.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * TitanKV client library.
 * Provides a high-level API for interacting with a TitanKV cluster.
 */
public class TitanKVClient implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(TitanKVClient.class);

    private final String[] hosts;
    private final ClientConfig config;
    private final ConnectionPool connectionPool;
    private final ConsistentHash hashRing;
    private volatile boolean closed = false;

    /**
     * Create a client connected to the specified hosts.
     *
     * @param hosts server addresses in host:port format
     */
    public TitanKVClient(String... hosts) {
        this(new ClientConfig(), hosts);
    }

    /**
     * Create a client with custom configuration.
     *
     * @param config the client configuration
     * @param hosts  server addresses in host:port format
     */
    public TitanKVClient(ClientConfig config, String... hosts) {
        if (hosts == null || hosts.length == 0) {
            throw new IllegalArgumentException("At least one host required");
        }

        this.hosts = hosts;
        this.config = config;
        this.connectionPool = new ConnectionPool(
            config.getMaxConnectionsPerHost(),
            config.getConnectTimeoutMs(),
            config.getReadTimeoutMs()
        );
        this.hashRing = new ConsistentHash();

        // Add all hosts to the hash ring
        for (String host : hosts) {
            Node node = Node.fromAddress(host);
            node.setStatus(Node.Status.ALIVE);
            hashRing.addNode(node);
        }

        logger.info("TitanKV client initialized with {} hosts", hosts.length);
    }

    /**
     * Get a value by key.
     *
     * @param key the key to retrieve
     * @return the value if found, empty otherwise
     * @throws IOException if the request fails
     */
    public Optional<byte[]> get(String key) throws IOException {
        validateKey(key);
        Response response = execute(Command.get(key), key);

        if (response.isOk() && response.hasValue()) {
            return Optional.of(response.getValue());
        }
        if (response.isNotFound()) {
            return Optional.empty();
        }
        if (response.isError()) {
            throw new IOException("Server error: " + response.getErrorMessage());
        }
        return Optional.empty();
    }

    /**
     * Get a value as a string.
     *
     * @param key the key to retrieve
     * @return the value as UTF-8 string if found, empty otherwise
     * @throws IOException if the request fails
     */
    public Optional<String> getString(String key) throws IOException {
        return get(key).map(bytes -> new String(bytes, StandardCharsets.UTF_8));
    }

    /**
     * Store a value.
     *
     * @param key   the key to store
     * @param value the value to store
     * @throws IOException if the request fails
     */
    public void put(String key, byte[] value) throws IOException {
        validateKey(key);
        Response response = execute(Command.put(key, value), key);

        if (response.isError()) {
            throw new IOException("Server error: " + response.getErrorMessage());
        }
    }

    /**
     * Store a string value.
     *
     * @param key   the key to store
     * @param value the string value to store (UTF-8 encoded)
     * @throws IOException if the request fails
     */
    public void put(String key, String value) throws IOException {
        put(key, value.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Delete a key.
     *
     * @param key the key to delete
     * @throws IOException if the request fails
     */
    public void delete(String key) throws IOException {
        validateKey(key);
        Response response = execute(Command.delete(key), key);

        if (response.isError()) {
            throw new IOException("Server error: " + response.getErrorMessage());
        }
    }

    /**
     * Check if a key exists.
     *
     * @param key the key to check
     * @return true if the key exists
     * @throws IOException if the request fails
     */
    public boolean exists(String key) throws IOException {
        validateKey(key);
        Response response = execute(Command.exists(key), key);

        if (response.isError()) {
            throw new IOException("Server error: " + response.getErrorMessage());
        }
        return response.getStatus() == Response.EXISTS_TRUE;
    }

    /**
     * Ping a server to check connectivity.
     *
     * @return true if the server responds
     */
    public boolean ping() {
        try {
            String host = hosts[0];
            Response response = executeOnHost(Command.ping(), host);
            return response.getStatus() == Response.PONG;
        } catch (IOException e) {
            logger.debug("Ping failed: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Execute a command with retry logic.
     */
    private Response execute(Command command, String key) throws IOException {
        ensureOpen();

        // Get the node for this key
        Node node = hashRing.getNode(key);
        String host = node.getAddress();

        int retries = config.isRetryOnFailure() ? config.getMaxRetries() : 1;
        IOException lastException = null;

        for (int attempt = 0; attempt < retries; attempt++) {
            try {
                return executeOnHost(command, host);
            } catch (IOException e) {
                lastException = e;
                logger.warn("Request failed (attempt {}/{}): {}",
                    attempt + 1, retries, e.getMessage());

                if (attempt < retries - 1) {
                    try {
                        Thread.sleep(config.getRetryDelayMs() * (attempt + 1));
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Interrupted during retry", ie);
                    }

                    // Try next node in the ring
                    List<Node> nodes = hashRing.getNodes(key, retries);
                    if (nodes.size() > attempt + 1) {
                        host = nodes.get(attempt + 1).getAddress();
                    }
                }
            }
        }

        throw new IOException("All retries failed", lastException);
    }

    /**
     * Execute a command on a specific host.
     */
    private Response executeOnHost(Command command, String host) throws IOException {
        PooledConnection conn = null;
        try {
            conn = connectionPool.acquire(host);

            // Send request
            ByteBuffer writeBuffer = conn.getWriteBuffer();
            BinaryProtocol.encode(command, writeBuffer);
            writeBuffer.flip();
            conn.write(writeBuffer);

            // Read response - buffer starts in write mode (position=0, limit=capacity)
            ByteBuffer readBuffer = conn.getReadBuffer();

            // Keep reading until we have a complete response
            while (true) {
                int read = conn.read(readBuffer);
                if (read == -1) {
                    throw new IOException("Connection closed by server");
                }

                // Flip to read mode to check if complete
                readBuffer.flip();

                if (BinaryProtocol.hasCompleteResponse(readBuffer)) {
                    break;
                }

                // Not complete - compact and continue reading
                readBuffer.compact();
            }

            return BinaryProtocol.decodeResponse(readBuffer);

        } catch (IOException e) {
            if (conn != null) {
                connectionPool.invalidate(conn);
                conn = null;
            }
            throw e;
        } finally {
            if (conn != null) {
                connectionPool.release(conn);
            }
        }
    }

    /**
     * Add a node to the client's view of the cluster.
     *
     * @param host the host address in host:port format
     */
    public void addNode(String host) {
        Node node = Node.fromAddress(host);
        node.setStatus(Node.Status.ALIVE);
        hashRing.addNode(node);
        logger.info("Added node {} to client", host);
    }

    /**
     * Remove a node from the client's view of the cluster.
     *
     * @param host the host address in host:port format
     */
    public void removeNode(String host) {
        Node node = Node.fromAddress(host);
        hashRing.removeNode(node);
        logger.info("Removed node {} from client", host);
    }

    /**
     * Get the number of nodes in the cluster.
     */
    public int getNodeCount() {
        return hashRing.getNodeCount();
    }

    /**
     * Get the number of active connections.
     */
    public int getConnectionCount() {
        return connectionPool.getTotalConnections();
    }

    private void validateKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Client is closed");
        }
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            connectionPool.close();
            logger.info("TitanKV client closed");
        }
    }

    /**
     * Check if the client is closed.
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * Command-line interface for testing.
     */
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: TitanKVClient <host:port> [command] [args...]");
            System.out.println("Commands:");
            System.out.println("  get <key>           - Get a value");
            System.out.println("  put <key> <value>   - Store a value");
            System.out.println("  delete <key>        - Delete a value");
            System.out.println("  ping                - Ping the server");
            return;
        }

        String host = args[0];

        try (TitanKVClient client = new TitanKVClient(host)) {
            if (args.length == 1 || args[1].equals("ping")) {
                boolean ok = client.ping();
                System.out.println(ok ? "PONG" : "Connection failed");
                return;
            }

            String command = args[1];

            switch (command.toLowerCase()) {
                case "get":
                    if (args.length < 3) {
                        System.out.println("Usage: get <key>");
                        return;
                    }
                    Optional<String> value = client.getString(args[2]);
                    if (value.isPresent()) {
                        System.out.println(value.get());
                    } else {
                        System.out.println("(nil)");
                    }
                    break;

                case "put":
                    if (args.length < 4) {
                        System.out.println("Usage: put <key> <value>");
                        return;
                    }
                    client.put(args[2], args[3]);
                    System.out.println("OK");
                    break;

                case "delete":
                    if (args.length < 3) {
                        System.out.println("Usage: delete <key>");
                        return;
                    }
                    client.delete(args[2]);
                    System.out.println("OK");
                    break;

                default:
                    System.out.println("Unknown command: " + command);
            }
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        }
    }
}
