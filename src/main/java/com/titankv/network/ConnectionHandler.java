package com.titankv.network;

import com.titankv.cluster.Node;
import com.titankv.consistency.ConsistencyLevel;
import com.titankv.core.KVStore;
import com.titankv.core.KeyValuePair;
import com.titankv.network.protocol.BinaryProtocol;
import com.titankv.network.protocol.Command;
import com.titankv.network.protocol.ProtocolException;
import com.titankv.network.protocol.Response;
import com.titankv.util.MetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

/**
 * Handles individual client connections.
 * Manages read/write buffers and request processing.
 */
public class ConnectionHandler {

    private static final Logger logger = LoggerFactory.getLogger(ConnectionHandler.class);
    private static final int BUFFER_SIZE = 64 * 1024;
    private static final int MAX_QUEUED_RESPONSES = 100;
    private static final int MAX_QUEUED_COMMANDS = 100;
    // Maximum read buffer size = header + max key + max value
    private static final int MAX_READ_BUFFER_SIZE = BinaryProtocol.REQUEST_HEADER_SIZE
            + BinaryProtocol.MAX_KEY_LENGTH + BinaryProtocol.MAX_VALUE_LENGTH;
    // Timeout for completing a request frame (30 seconds)
    private static final long INCOMPLETE_FRAME_TIMEOUT_MS = 30_000;

    private final SocketChannel channel;
    private final KVStore store;
    private final MetricsCollector metrics;
    private final com.titankv.consistency.ReplicationManager replicationManager;
    private final com.titankv.cluster.ClusterManager clusterManager;
    private final TcpServer server;
    private final ExecutorService workerPool;
    private final Selector selector;
    private ByteBuffer readBuffer; // Now mutable to allow growth
    private final ByteBuffer writeBuffer;
    private final String clientAddress;
    private final Queue<ByteBuffer> pendingResponses;
    private final Queue<Command> pendingCommands; // Queue of commands awaiting processing
    private ByteBuffer currentResponse; // Track partial write progress
    private boolean closed = false; // Track if connection is already closed
    private SelectionKey selectionKey; // Track selection key for async wakeup
    private boolean commandInProgress = false; // Track if a command is currently being processed
    private boolean writeInProgress = false;

    // Frame timeout tracking to prevent memory DoS
    private long incompleteFrameStartTime = 0; // When we first received data for current incomplete frame
    private boolean hasIncompleteFrame = false; // Whether we're currently waiting for more data

    // Lock for protecting interestOps modifications (prevents race conditions)
    private final Object interestOpsLock = new Object();

    private static final ConcurrentMap<String, String> HOST_IP_CACHE = new ConcurrentHashMap<>();
    private static final String CLIENT_AUTH_TOKEN = readToken("TITANKV_CLIENT_TOKEN", "titankv.client.token");
    private static final String INTERNAL_AUTH_TOKEN = readInternalToken();
    private static final boolean CLIENT_AUTH_REQUIRED = CLIENT_AUTH_TOKEN != null && !CLIENT_AUTH_TOKEN.isEmpty();
    private static final boolean INTERNAL_AUTH_REQUIRED = INTERNAL_AUTH_TOKEN != null && !INTERNAL_AUTH_TOKEN.isEmpty();
    private static final java.util.concurrent.atomic.AtomicLong LAST_TIMESTAMP = new java.util.concurrent.atomic.AtomicLong();
    private static final ConsistencyLevel READ_CONSISTENCY = readConsistencyLevel(
            "TITANKV_READ_CONSISTENCY", "titankv.read.consistency", ConsistencyLevel.QUORUM);
    private static final ConsistencyLevel WRITE_CONSISTENCY = readConsistencyLevel(
            "TITANKV_WRITE_CONSISTENCY", "titankv.write.consistency", ConsistencyLevel.QUORUM);
    private static final ConsistencyLevel DELETE_CONSISTENCY = readConsistencyLevel(
            "TITANKV_DELETE_CONSISTENCY", "titankv.delete.consistency", ConsistencyLevel.QUORUM);

    private boolean clientAuthenticated = false;
    private boolean internalAuthenticated = false;

    public ConnectionHandler(SocketChannel channel, KVStore store, MetricsCollector metrics,
            com.titankv.consistency.ReplicationManager replicationManager,
            com.titankv.cluster.ClusterManager clusterManager,
            TcpServer server, ExecutorService workerPool, Selector selector) {
        this.channel = channel;
        this.store = store;
        this.metrics = metrics;
        this.replicationManager = replicationManager;
        this.clusterManager = clusterManager;
        this.server = server;
        this.workerPool = workerPool;
        this.selector = selector;
        this.readBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
        this.writeBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
        this.clientAddress = getClientAddress();
        this.pendingResponses = new LinkedList<>();
        this.pendingCommands = new LinkedList<>();
        metrics.connectionOpened();
        logger.debug("New connection from {}", clientAddress);
    }

    private String getClientAddress() {
        try {
            return channel.getRemoteAddress().toString();
        } catch (IOException e) {
            return "unknown";
        }
    }

    /**
     * Check if this connection is authorized to send internal commands.
     * Internal commands should only come from known cluster nodes.
     *
     * PERFORMANCE: Uses direct IP comparison against ClusterManager's cached
     * node list. Does NOT perform any DNS lookups to avoid blocking.
     *
     * @return true if authorized, false otherwise
     */
    private boolean isAuthorizedForInternalCommands() {
        // If no cluster manager, single-node mode - no internal commands expected
        if (clusterManager == null) {
            logger.warn("Internal command rejected: no cluster configured from {}", clientAddress);
            return false;
        }

        try {
            // Get the remote address (no DNS lookup - just cached socket info)
            java.net.InetSocketAddress remoteAddr = (java.net.InetSocketAddress) channel.getRemoteAddress();
            if (remoteAddr == null) {
                logger.warn("Internal command rejected: cannot determine remote address");
                return false;
            }

            String remoteIP = remoteAddr.getAddress().getHostAddress();

            // Check if this IP belongs to a known cluster node
            // ClusterManager maintains a cached list - no network calls
            boolean authorized = clusterManager.getAllNodes().stream()
                    .anyMatch(node -> {
                        String nodeHost = node.getHost();
                        // Direct IP match (most common case)
                        if (remoteIP.equals(node.getHashHost()) || remoteIP.equals(nodeHost)) {
                            return true;
                        }
                        String resolved = resolveHostIp(nodeHost);
                        if (remoteIP.equals(resolved)) {
                            return true;
                        }
                        // Handle localhost variations
                        if (isLocalhost(remoteIP) && isLocalhost(nodeHost)) {
                            return true;
                        }
                        return false;
                    });

            if (!authorized) {
                logger.warn("Internal command rejected: unauthorized IP {} not in cluster", remoteIP);
            }

            return authorized;
        } catch (IOException e) {
            logger.warn("Internal command rejected: error getting remote address: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Check if an address is a localhost variant.
     * Handles 127.0.0.1, ::1, and "localhost" string.
     */
    private boolean isLocalhost(String addr) {
        return "127.0.0.1".equals(addr)
                || "::1".equals(addr)
                || "0:0:0:0:0:0:0:1".equals(addr)
                || "localhost".equalsIgnoreCase(addr);
    }

    private String resolveHostIp(String host) {
        if (host == null || host.isEmpty()) {
            return host;
        }
        return HOST_IP_CACHE.computeIfAbsent(host, value -> {
            try {
                return InetAddress.getByName(value).getHostAddress();
            } catch (Exception e) {
                return value;
            }
        });
    }

    private static String readToken(String envKey, String propKey) {
        String value = System.getenv(envKey);
        if (value == null || value.isEmpty()) {
            value = System.getProperty(propKey);
        }
        return value != null && !value.isEmpty() ? value : null;
    }

    private static String readInternalToken() {
        String value = readToken("TITANKV_INTERNAL_TOKEN", "titankv.internal.token");
        if (value == null) {
            value = readToken("TITANKV_CLUSTER_SECRET", "titankv.cluster.secret");
        }
        return value;
    }

    private static ConsistencyLevel readConsistencyLevel(String envKey, String propKey, ConsistencyLevel fallback) {
        String value = readToken(envKey, propKey);
        if (value == null) {
            return fallback;
        }
        try {
            return ConsistencyLevel.valueOf(value.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            logger.warn("Invalid consistency level {} (using {})", value, fallback);
            return fallback;
        }
    }

    private boolean isInternalCommand(byte type) {
        return type == Command.GET_INTERNAL
                || type == Command.PUT_INTERNAL
                || type == Command.DELETE_INTERNAL;
    }

    private Response authorizeCommand(Command command) {
        byte type = command.getType();
        if (type == Command.AUTH) {
            return null;
        }
        if (isInternalCommand(type)) {
            if (INTERNAL_AUTH_REQUIRED && !internalAuthenticated) {
                return Response.error("AUTH required for internal commands");
            }
            if (!isAuthorizedForInternalCommands()) {
                return Response.error("Unauthorized internal command");
            }
            return null;
        }
        if (CLIENT_AUTH_REQUIRED && !clientAuthenticated) {
            return Response.error("AUTH required");
        }
        return null;
    }

    private static long nextTimestamp() {
        long now = System.currentTimeMillis();
        while (true) {
            long last = LAST_TIMESTAMP.get();
            long next = Math.max(now, last + 1);
            if (LAST_TIMESTAMP.compareAndSet(last, next)) {
                return next;
            }
        }
    }

    private Response handleAuth(Command command) {
        if (!CLIENT_AUTH_REQUIRED && !INTERNAL_AUTH_REQUIRED) {
            return Response.ok();
        }
        byte[] tokenBytes = command.getValueUnsafe();
        if (tokenBytes == null || tokenBytes.length == 0) {
            return Response.error("AUTH requires token");
        }
        String token = new String(tokenBytes, java.nio.charset.StandardCharsets.UTF_8);
        boolean matched = false;
        if (CLIENT_AUTH_REQUIRED && token.equals(CLIENT_AUTH_TOKEN)) {
            clientAuthenticated = true;
            matched = true;
        }
        if (INTERNAL_AUTH_REQUIRED && token.equals(INTERNAL_AUTH_TOKEN)) {
            internalAuthenticated = true;
            matched = true;
        }
        if (!matched) {
            return Response.error("Authentication failed");
        }
        return Response.ok();
    }

    private Response maybeRedirect(String key) {
        if (clusterManager == null || key == null) {
            return null;
        }
        if (clusterManager.getNodeCount() <= 1) {
            return null;
        }
        try {
            Node primary = clusterManager.getNodeForKey(key);
            Node local = clusterManager.getLocalNode();
            if (primary != null && local != null && !primary.equals(local)) {
                return Response.error("MOVED " + primary.getAddress());
            }
        } catch (RuntimeException e) {
            logger.debug("Owner check failed for key {}: {}", key, e.getMessage());
        }
        return null;
    }

    /**
     * Handle a read event from the selector.
     *
     * @param key the selection key
     * @return true if the connection should continue, false to close
     */
    public boolean handleRead(SelectionKey key) {
        // Store selection key for async operations
        if (this.selectionKey == null) {
            this.selectionKey = key;
        }

        try {
            int bytesRead = channel.read(readBuffer);
            if (bytesRead == -1) {
                logger.debug("Client {} disconnected", clientAddress);
                return false;
            }

            if (bytesRead > 0) {
                // Track incomplete frame for timeout detection
                readBuffer.flip();
                boolean hasCompleteFrame = readBuffer.remaining() > 0 && BinaryProtocol.hasCompleteRequest(readBuffer);
                readBuffer.compact();

                if (!hasCompleteFrame && readBuffer.position() > 0) {
                    // We have data but no complete frame yet
                    if (!hasIncompleteFrame) {
                        // First time we've received data for this incomplete frame
                        hasIncompleteFrame = true;
                        incompleteFrameStartTime = System.currentTimeMillis();
                        logger.trace("Started tracking incomplete frame from {}", clientAddress);
                    } else {
                        // Check if we've exceeded the timeout
                        long elapsed = System.currentTimeMillis() - incompleteFrameStartTime;
                        if (elapsed > INCOMPLETE_FRAME_TIMEOUT_MS) {
                            logger.warn("Connection from {} exceeded incomplete frame timeout ({}ms), closing",
                                    clientAddress, elapsed);
                            return false;
                        }
                    }
                }

                processReadBuffer(key);

                // Reset incomplete frame tracking if we processed a complete request
                if (hasCompleteFrame) {
                    hasIncompleteFrame = false;
                    incompleteFrameStartTime = 0;
                }

                // After processing, buffer is in write mode (position=end of unprocessed data)
                // If buffer is completely full but no complete request, need to grow
                if (readBuffer.position() == readBuffer.capacity()) {
                    // Buffer is full - check if there's a complete request
                    readBuffer.flip();
                    boolean hasComplete = BinaryProtocol.hasCompleteRequest(readBuffer);
                    readBuffer.compact(); // Back to write mode

                    if (!hasComplete) {
                        // Buffer full with incomplete request - must grow
                        growReadBuffer();
                    }
                }
            }
            return true;
        } catch (ProtocolException e) {
            logger.warn("Protocol violation from {} (closing connection): {}",
                    clientAddress, e.getMessage());
            return false;
        } catch (IOException e) {
            logger.warn("Read error from {}: {}", clientAddress, e.getMessage());
            return false;
        }
    }

    private void processReadBuffer(SelectionKey key) {
        readBuffer.flip();

        while (BinaryProtocol.hasCompleteRequest(readBuffer)) {
            try {
                Command command = BinaryProtocol.decodeCommand(readBuffer);
                // Queue command for sequential processing to preserve ordering
                synchronized (pendingCommands) {
                    if (pendingCommands.size() >= MAX_QUEUED_COMMANDS) {
                        logger.error("Command queue overflow for {}, closing connection", clientAddress);
                        key.cancel();
                        close();
                        return;
                    }
                    pendingCommands.offer(command);
                }
                // Trigger processing if no command is currently being processed
                processNextCommand(key);
            } catch (ProtocolException e) {
                logger.warn("Protocol error from {}: {}", clientAddress, e.getMessage());
                queueResponse(Response.error(e.getMessage()), key);
            }
        }

        readBuffer.compact();
    }

    /**
     * Process the next command in the queue if one is not already being processed.
     * This ensures per-connection ordering: commands are processed one at a time.
     */
    private void processNextCommand(SelectionKey key) {
        synchronized (pendingCommands) {
            if (commandInProgress || pendingCommands.isEmpty()) {
                return; // Already processing or no work to do
            }

            Command command = pendingCommands.poll();
            if (command != null) {
                commandInProgress = true;
                // Offload to worker pool for async processing
                workerPool.submit(() -> {
                    try {
                        processCommandAsync(command);
                    } finally {
                        // Mark this command as complete and process next
                        synchronized (pendingCommands) {
                            commandInProgress = false;
                        }
                        processNextCommand(key);
                    }
                });
            }
        }
    }

    /**
     * Process command on worker thread.
     * Maintains per-connection ordering by waiting for replicated ops to finish
     * before queuing the response.
     */
    private void processCommandAsync(Command command) {
        long startTime = System.nanoTime();

        try {
            if (command.getType() == Command.AUTH) {
                Response authResponse = handleAuth(command);
                queueResponseAsync(authResponse);
                return;
            }

            Response authError = authorizeCommand(command);
            if (authError != null) {
                queueResponseAsync(authError);
                metrics.recordError();
                return;
            }

            switch (command.getType()) {
                case Command.GET:
                    Response getResponse = handleGetBlocking(command);
                    metrics.recordGet(System.nanoTime() - startTime, getResponse.getStatus() == Response.OK);
                    queueResponseAsync(getResponse);
                    return;

                case Command.PUT:
                    Response putResponse = handlePutBlocking(command);
                    metrics.recordPut(System.nanoTime() - startTime);
                    queueResponseAsync(putResponse);
                    return;

                case Command.DELETE:
                    Response deleteResponse = handleDeleteBlocking(command);
                    metrics.recordDelete(System.nanoTime() - startTime);
                    queueResponseAsync(deleteResponse);
                    return;

                // Sync handlers below - we queue the response
                case Command.GET_INTERNAL:
                    Response response = handleGetInternal(command);
                    metrics.recordGet(System.nanoTime() - startTime, response.getStatus() == Response.OK);
                    queueResponseAsync(response);
                    return;

                case Command.PUT_INTERNAL:
                    response = handlePutInternal(command);
                    metrics.recordPut(System.nanoTime() - startTime);
                    queueResponseAsync(response);
                    return;

                case Command.DELETE_INTERNAL:
                    response = handleDeleteInternal(command);
                    metrics.recordDelete(System.nanoTime() - startTime);
                    queueResponseAsync(response);
                    return;

                case Command.PING:
                    queueResponseAsync(Response.pong());
                    return;

                case Command.EXISTS:
                    response = handleExists(command);
                    queueResponseAsync(response);
                    return;

                case Command.KEYS:
                    logger.info("KEYS command received but is disabled in distributed mode");
                    queueResponseAsync(
                            Response.error("KEYS command is disabled in distributed mode for performance reasons"));
                    return;

                default:
                    logger.warn("Unknown command type: {}", command.getType());
                    queueResponseAsync(Response.error("Unknown command"));
                    metrics.recordError();
            }
        } catch (IllegalArgumentException e) {
            logger.warn("Invalid argument for command {}: {}", command.getTypeName(), e.getMessage());
            queueResponseAsync(Response.error("Invalid argument: " + e.getMessage()));
            metrics.recordError();
        } catch (IllegalStateException e) {
            logger.error("State error processing command {}: {}", command.getTypeName(), e.getMessage());
            queueResponseAsync(Response.error("Internal error: " + e.getMessage()));
            metrics.recordError();
        } catch (RuntimeException e) {
            logger.error("Error processing command {}: {}", command.getTypeName(), e.toString(), e);
            queueResponseAsync(Response.error("Internal error: " + e.getMessage()));
            metrics.recordError();
        }
    }

    /**
     * Handle GET command and return response. Blocks for replicated reads to
     * preserve per-connection ordering.
     */
    private Response handleGetBlocking(Command command) {
        if (command.getKey() == null) {
            return Response.error("Key required for GET");
        }

        Response moved = maybeRedirect(command.getKey());
        if (moved != null) {
            return moved;
        }

        // Use distributed read if replication is enabled and cluster has multiple nodes
        if (replicationManager != null && clusterManager != null && clusterManager.getAliveNodeCount() > 1) {
            try {
                Optional<com.titankv.consistency.ReplicationManager.ReadResult> result = replicationManager
                        .read(command.getKey(), READ_CONSISTENCY)
                        .get(5, java.util.concurrent.TimeUnit.SECONDS);
                if (result.isPresent()) {
                    com.titankv.consistency.ReplicationManager.ReadResult readResult = result.get();
                    if (readResult.getValue() == null) {
                        return Response.notFound();
                    }
                    return Response.ok(readResult.getValue(), readResult.getTimestamp(),
                            readResult.getExpiresAt());
                }
                return Response.notFound();
            } catch (java.util.concurrent.TimeoutException e) {
                logger.warn("Read timeout for key {}", command.getKey());
                return Response.error("Read timeout");
            } catch (java.util.concurrent.ExecutionException e) {
                logger.warn("Read failed for key {}: {}", command.getKey(), e.getMessage());
                return Response.error("Read failed: " + e.getMessage());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return Response.error("Read interrupted");
            }
        }

        // Single-node mode: local read
        Optional<KeyValuePair> result = store.get(command.getKey());
        if (result.isPresent()) {
            KeyValuePair kv = result.get();
            return Response.ok(kv.getValueUnsafe(), kv.getTimestamp(), kv.getExpiresAt());
        }
        return Response.notFound();
    }

    /**
     * Handle internal GET command from replication (local read only).
     * SECURITY: Only authorized cluster nodes can use this command.
     */
    private Response handleGetInternal(Command command) {
        // Validate authorization for internal commands
        if (!isAuthorizedForInternalCommands()) {
            return Response.error("Unauthorized: internal commands require cluster membership");
        }

        if (command.getKey() == null) {
            return Response.error("Key required for GET");
        }
        // Read directly from local store without replication (include tombstones)
        Optional<KeyValuePair> result = store.getRaw(command.getKey());
        if (result.isPresent()) {
            KeyValuePair kv = result.get();
            return Response.ok(kv.getValueUnsafe(), kv.getTimestamp(), kv.getExpiresAt());
        }
        return Response.notFound();
    }

    /**
     * Handle PUT command and return response. Blocks for replicated writes to
     * preserve per-connection ordering.
     */
    private Response handlePutBlocking(Command command) {
        if (command.getKey() == null) {
            return Response.error("Key required for PUT");
        }

        Response moved = maybeRedirect(command.getKey());
        if (moved != null) {
            return moved;
        }

        long timestamp = nextTimestamp();
        long expiresAt = command.getExpiresAt();

        // Use distributed write if replication is enabled and cluster has multiple
        // nodes
        if (replicationManager != null && clusterManager != null && clusterManager.getAliveNodeCount() > 1) {
            try {
                replicationManager
                        .write(command.getKey(), command.getValueUnsafe(), timestamp, expiresAt, WRITE_CONSISTENCY)
                        .get(5, java.util.concurrent.TimeUnit.SECONDS);
                return Response.ok();
            } catch (java.util.concurrent.TimeoutException e) {
                logger.warn("Write timeout for key {}", command.getKey());
                return Response.error("Write timeout");
            } catch (java.util.concurrent.ExecutionException e) {
                logger.warn("Write failed for key {}: {}", command.getKey(), e.getMessage());
                return Response.error("Write failed: " + e.getMessage());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return Response.error("Write interrupted");
            }
        }

        // Single-node mode - store locally
        store.put(command.getKey(), command.getValueUnsafe());
        return Response.ok();
    }

    /**
     * Handle internal PUT command from replication (no further replication).
     * Uses timestamp-aware putIfNewer to maintain conflict resolution semantics.
     * SECURITY: Only authorized cluster nodes can use this command.
     */
    private Response handlePutInternal(Command command) {
        // Validate authorization for internal commands
        if (!isAuthorizedForInternalCommands()) {
            return Response.error("Unauthorized: internal commands require cluster membership");
        }

        if (command.getKey() == null) {
            return Response.error("Key required for PUT");
        }

        // Validate timestamp is provided for internal writes
        if (command.getTimestamp() == 0) {
            return Response.error("Timestamp required for internal PUT");
        }

        // Write using timestamp-aware putIfNewer to maintain newest-wins semantics
        boolean written = ((com.titankv.core.InMemoryStore) store).putIfNewer(
                command.getKey(),
                command.getValueUnsafe(),
                command.getTimestamp(),
                command.getExpiresAt());

        if (written) {
            logger.trace("PUT_INTERNAL accepted: key={}, timestamp={}",
                    command.getKey(), command.getTimestamp());
        } else {
            logger.trace("PUT_INTERNAL rejected (stale): key={}, timestamp={}",
                    command.getKey(), command.getTimestamp());
        }

        return Response.ok();
    }

    /**
     * Handle DELETE command and return response. Blocks for replicated deletes to
     * preserve per-connection ordering.
     */
    private Response handleDeleteBlocking(Command command) {
        if (command.getKey() == null) {
            return Response.error("Key required for DELETE");
        }

        Response moved = maybeRedirect(command.getKey());
        if (moved != null) {
            return moved;
        }

        // Create tombstone timestamp (ensure it's newer than any existing entry)
        Optional<KeyValuePair> existing = store.getRaw(command.getKey());
        long timestamp = nextTimestamp();
        if (existing.isPresent()) {
            timestamp = Math.max(timestamp, existing.get().getTimestamp() + 1);
        }

        long expiresAt = 0; // Tombstones don't expire
        // Use distributed delete if replication is enabled and cluster has multiple
        // nodes
        if (replicationManager != null && clusterManager != null && clusterManager.getAliveNodeCount() > 1) {
            try {
                replicationManager
                        .delete(command.getKey(), timestamp, expiresAt, DELETE_CONSISTENCY)
                        .get(5, java.util.concurrent.TimeUnit.SECONDS);
                return Response.ok();
            } catch (java.util.concurrent.TimeoutException e) {
                logger.warn("Delete timeout for key {}", command.getKey());
                return Response.error("Delete timeout");
            } catch (java.util.concurrent.ExecutionException e) {
                logger.warn("Delete failed for key {}: {}", command.getKey(), e.getMessage());
                return Response.error("Delete failed: " + e.getMessage());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return Response.error("Delete interrupted");
            }
        }

        // Single-node mode - write tombstone locally
        boolean written = ((com.titankv.core.InMemoryStore) store).putIfNewer(
                command.getKey(),
                null,
                timestamp,
                expiresAt);
        if (!written) {
            logger.warn("Failed to write tombstone for key {} (concurrent modification?)", command.getKey());
        }
        return Response.ok();
    }

    /**
     * Handle internal DELETE command from replication (no further replication).
     * Uses tombstone with timestamp to prevent resurrection of deleted values.
     * SECURITY: Only authorized cluster nodes can use this command.
     */
    private Response handleDeleteInternal(Command command) {
        // Validate authorization for internal commands
        if (!isAuthorizedForInternalCommands()) {
            return Response.error("Unauthorized: internal commands require cluster membership");
        }

        if (command.getKey() == null) {
            return Response.error("Key required for DELETE");
        }

        // Validate timestamp is provided for internal deletes
        if (command.getTimestamp() == 0) {
            return Response.error("Timestamp required for internal DELETE");
        }

        // Write tombstone (null value with timestamp) using putIfNewer
        // This prevents stale replicas from resurrecting the deleted value
        boolean written = ((com.titankv.core.InMemoryStore) store).putIfNewer(
                command.getKey(),
                null, // null value = tombstone
                command.getTimestamp(),
                command.getExpiresAt());

        if (written) {
            logger.trace("DELETE_INTERNAL tombstone written: key={}, timestamp={}",
                    command.getKey(), command.getTimestamp());
        } else {
            logger.trace("DELETE_INTERNAL tombstone rejected (stale): key={}, timestamp={}",
                    command.getKey(), command.getTimestamp());
        }

        return Response.ok();
    }

    private Response handleExists(Command command) {
        if (command.getKey() == null) {
            return Response.error("Key required for EXISTS");
        }
        Response moved = maybeRedirect(command.getKey());
        if (moved != null) {
            return moved;
        }
        return Response.exists(store.exists(command.getKey()));
    }

    /**
     * Queue response from selector thread.
     * Thread-safe - synchronized to protect shared state.
     */
    private void queueResponse(Response response, SelectionKey key) {
        ByteBuffer encoded = BinaryProtocol.encode(response);

        synchronized (this) {
            if (closed) {
                return; // Connection closed, discard response
            }

            if (pendingResponses.size() >= MAX_QUEUED_RESPONSES) {
                logger.error("Response queue overflow for {}, closing connection", clientAddress);
                key.cancel();
                close();
                return;
            }

            pendingResponses.offer(encoded);

            if (!writeInProgress) {
                drainToWriteBuffer();
                if (writeBuffer.position() > 0) {
                    writeInProgress = true;
                    // Synchronized to prevent race with queueResponse from worker threads
                    synchronized (interestOpsLock) {
                        if (key.isValid()) {
                            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                        }
                    }
                }
            }
        }
    }

    /**
     * Queue response from worker thread and wake up selector.
     * Thread-safe - can be called from any worker thread.
     */
    private void queueResponseAsync(Response response) {
        ByteBuffer encoded = BinaryProtocol.encode(response);

        synchronized (this) {
            if (closed) {
                return; // Connection closed, discard response
            }

            if (pendingResponses.size() >= MAX_QUEUED_RESPONSES) {
                logger.error("Response queue overflow for {}, closing connection", clientAddress);
                close();
                return;
            }

            pendingResponses.offer(encoded);

            // Register write interest if not already in progress
            if (!writeInProgress && selectionKey != null && selectionKey.isValid()) {
                writeInProgress = true;
                // Synchronized to prevent race with selector thread modifying interestOps
                synchronized (interestOpsLock) {
                    if (selectionKey.isValid()) {
                        selectionKey.interestOps(selectionKey.interestOps() | SelectionKey.OP_WRITE);
                    }
                }
                // Wake up selector so it notices the interest ops change
                selector.wakeup();
            }
        }
    }

    private void drainToWriteBuffer() {
        while (writeBuffer.hasRemaining()) {
            // If no current response, get next from queue
            if (currentResponse == null || !currentResponse.hasRemaining()) {
                currentResponse = pendingResponses.poll();
                if (currentResponse == null) {
                    break; // No more responses to write
                }
            }

            // Write as much as possible from current response
            int toWrite = Math.min(writeBuffer.remaining(), currentResponse.remaining());
            if (toWrite > 0) {
                int oldLimit = currentResponse.limit();
                currentResponse.limit(currentResponse.position() + toWrite);
                writeBuffer.put(currentResponse);
                currentResponse.limit(oldLimit);
            }
        }
    }

    /**
     * Handle a write event from the selector.
     * Thread-safe - synchronized with queueResponseAsync.
     *
     * @param key the selection key
     * @return true if the connection should continue, false to close
     */
    public boolean handleWrite(SelectionKey key) {
        synchronized (this) {
            try {
                writeBuffer.flip();
                channel.write(writeBuffer);
                writeBuffer.compact();

                // Try to drain more pending responses into the write buffer
                drainToWriteBuffer();

                // Done writing when buffer is empty, no queued responses, and current response
                // is complete
                boolean allWritten = writeBuffer.position() == 0
                        && pendingResponses.isEmpty()
                        && (currentResponse == null || !currentResponse.hasRemaining());

                if (allWritten) {
                    writeInProgress = false;
                    currentResponse = null; // Clear reference
                    // Synchronized to prevent race with queueResponse from worker threads
                    synchronized (interestOpsLock) {
                        if (key.isValid()) {
                            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
                        }
                    }
                }
                return true;
            } catch (IOException e) {
                logger.warn("Write error to {}: {}", clientAddress, e.getMessage());
                return false;
            }
        }
    }

    /**
     * Grow the read buffer to accommodate larger requests.
     * PRECONDITION: Buffer must be in WRITE MODE (after compact or initial state).
     * POSTCONDITION: Buffer is in WRITE MODE with all unread bytes preserved at the
     * start.
     */
    private void growReadBuffer() {
        int currentCapacity = readBuffer.capacity();
        if (currentCapacity >= MAX_READ_BUFFER_SIZE) {
            throw new IllegalStateException("Read buffer at maximum size " + currentCapacity +
                    ", request too large for " + clientAddress);
        }

        // Current buffer is in write mode: position=end of data, limit=capacity
        // We need to preserve bytes from [0, position)
        int preservedBytes = readBuffer.position();

        // Double the size, but cap at MAX_READ_BUFFER_SIZE
        long newCapacity = Math.min((long) currentCapacity * 2, MAX_READ_BUFFER_SIZE);
        logger.debug("Growing read buffer from {} to {} bytes for {} (preserving {} bytes)",
                currentCapacity, newCapacity, clientAddress, preservedBytes);

        // Allocate new buffer and copy existing data
        ByteBuffer newBuffer = ByteBuffer.allocateDirect((int) newCapacity);

        // Copy preserved bytes: flip to read mode, copy, then buffer is back in write
        // mode
        readBuffer.flip();
        newBuffer.put(readBuffer);

        // newBuffer is now in write mode with position at end of copied data
        readBuffer = newBuffer;
    }

    /**
     * Close this connection and release resources.
     * Idempotent and thread-safe - safe to call multiple times from any thread.
     */
    public void close() {
        synchronized (this) {
            if (closed) {
                return; // Already closed
            }
            closed = true;
        }

        try {
            channel.close();
        } catch (IOException e) {
            logger.debug("Error closing connection: {}", e.getMessage());
        }

        // Remove from server's connection tracking
        if (server != null) {
            server.removeConnection(channel);
        }

        metrics.connectionClosed();
        logger.debug("Connection closed: {}", clientAddress);
    }

    public String getRemoteAddress() {
        return clientAddress;
    }
}
