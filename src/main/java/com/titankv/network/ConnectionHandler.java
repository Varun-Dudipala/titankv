package com.titankv.network;

import com.titankv.consistency.ConsistencyLevel;
import com.titankv.core.KVStore;
import com.titankv.core.KeyValuePair;
import com.titankv.network.protocol.BinaryProtocol;
import com.titankv.network.protocol.Command;
import com.titankv.network.protocol.ProtocolException;
import com.titankv.network.protocol.Response;
import com.titankv.util.Env;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
    private static final long OPERATION_TIMEOUT_MS = 5_000;

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
    private static final java.util.concurrent.atomic.AtomicLong LAST_TIMESTAMP = new java.util.concurrent.atomic.AtomicLong();

    // Read per connection (not statically) so servers started with different settings
    // in the same JVM, e.g. in tests, each get their own configuration.
    private final String clientAuthToken = Env.clientToken();
    private final String internalAuthToken = Env.internalToken();
    private final boolean clientAuthRequired = clientAuthToken != null;
    private final boolean internalAuthRequired = internalAuthToken != null;
    private final ConsistencyLevel readConsistency = readConsistencyLevel(
            "TITANKV_READ_CONSISTENCY", "titankv.read.consistency", ConsistencyLevel.QUORUM);
    private final ConsistencyLevel writeConsistency = readConsistencyLevel(
            "TITANKV_WRITE_CONSISTENCY", "titankv.write.consistency", ConsistencyLevel.QUORUM);
    private final ConsistencyLevel deleteConsistency = readConsistencyLevel(
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

    private static ConsistencyLevel readConsistencyLevel(String envKey, String propKey, ConsistencyLevel fallback) {
        String value = Env.get(envKey, propKey);
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
            if (internalAuthRequired && !internalAuthenticated) {
                return Response.error("AUTH required for internal commands");
            }
            if (!isAuthorizedForInternalCommands()) {
                return Response.error("Unauthorized internal command");
            }
            return null;
        }
        if (clientAuthRequired && !clientAuthenticated) {
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

    private static boolean tokenMatches(String provided, String expected) {
        return java.security.MessageDigest.isEqual(
                provided.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                expected.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private Response handleAuth(Command command) {
        if (!clientAuthRequired && !internalAuthRequired) {
            return Response.ok();
        }
        byte[] tokenBytes = command.getValueUnsafe();
        if (tokenBytes == null || tokenBytes.length == 0) {
            return Response.error("AUTH requires token");
        }
        String token = new String(tokenBytes, java.nio.charset.StandardCharsets.UTF_8);
        boolean matched = false;
        if (clientAuthRequired && tokenMatches(token, clientAuthToken)) {
            clientAuthenticated = true;
            matched = true;
        }
        if (internalAuthRequired && tokenMatches(token, internalAuthToken)) {
            internalAuthenticated = true;
            matched = true;
        }
        if (!matched) {
            return Response.error("Authentication failed");
        }
        return Response.ok();
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
     * Commands on a connection run one at a time, so responses keep request order.
     * The worker thread never waits on replicas: replicated operations complete
     * asynchronously and the next command is scheduled from the completion callback.
     */
    private void processNextCommand(SelectionKey key) {
        Command command;
        synchronized (pendingCommands) {
            if (commandInProgress || pendingCommands.isEmpty()) {
                return;
            }
            command = pendingCommands.poll();
            commandInProgress = true;
        }
        workerPool.submit(() -> {
            long startTime = System.nanoTime();
            CompletableFuture<Response> pending;
            try {
                pending = dispatch(command);
            } catch (RuntimeException e) {
                pending = CompletableFuture.completedFuture(errorResponse(command, e));
            }
            pending.whenComplete((response, error) -> {
                Response result = error != null ? errorResponse(command, unwrap(error)) : response;
                recordMetrics(command, result, System.nanoTime() - startTime);
                queueResponseAsync(result);
                synchronized (pendingCommands) {
                    commandInProgress = false;
                }
                processNextCommand(key);
            });
        });
    }

    private CompletableFuture<Response> dispatch(Command command) {
        if (command.getType() == Command.AUTH) {
            return done(handleAuth(command));
        }
        Response authError = authorizeCommand(command);
        if (authError != null) {
            return done(authError);
        }
        switch (command.getType()) {
            case Command.GET:
                return handleGet(command);
            case Command.PUT:
                return handlePut(command);
            case Command.DELETE:
                return handleDelete(command);
            case Command.EXISTS:
                return handleExists(command);
            case Command.GET_INTERNAL:
                return done(handleGetInternal(command));
            case Command.PUT_INTERNAL:
                return done(handlePutInternal(command));
            case Command.DELETE_INTERNAL:
                return done(handleDeleteInternal(command));
            case Command.PING:
                return done(Response.pong());
            case Command.KEYS:
                return done(Response.error("KEYS command is disabled in distributed mode for performance reasons"));
            default:
                logger.warn("Unknown command type: {}", command.getType());
                return done(Response.error("Unknown command"));
        }
    }

    private static CompletableFuture<Response> done(Response response) {
        return CompletableFuture.completedFuture(response);
    }

    private static Throwable unwrap(Throwable error) {
        while ((error instanceof CompletionException || error instanceof ExecutionException)
                && error.getCause() != null) {
            error = error.getCause();
        }
        return error;
    }

    private Response errorResponse(Command command, Throwable error) {
        if (error instanceof IllegalArgumentException) {
            logger.warn("Invalid argument for command {}: {}", command.getTypeName(), error.getMessage());
            return Response.error("Invalid argument: " + error.getMessage());
        }
        logger.error("Error processing command {}: {}", command.getTypeName(), error.toString(), error);
        return Response.error("Internal error: " + error.getMessage());
    }

    private void recordMetrics(Command command, Response response, long durationNanos) {
        switch (command.getType()) {
            case Command.GET:
            case Command.GET_INTERNAL:
                metrics.recordGet(durationNanos, response.getStatus() == Response.OK);
                break;
            case Command.PUT:
            case Command.PUT_INTERNAL:
                metrics.recordPut(durationNanos);
                break;
            case Command.DELETE:
            case Command.DELETE_INTERNAL:
                metrics.recordDelete(durationNanos);
                break;
            default:
                break;
        }
        if (response.isError()) {
            metrics.recordError();
        }
    }

    /**
     * Whether requests go through replication. Based on membership, not liveness, so a node
     * whose peers are down fails QUORUM requests instead of falling back to a local-only write.
     */
    private boolean isDistributed() {
        return replicationManager != null && clusterManager != null && clusterManager.getNodeCount() > 1;
    }

    /**
     * Maps a replicated operation's outcome to a response, failing it if it runs past the timeout.
     */
    private <T> CompletableFuture<Response> replicated(CompletableFuture<T> operation, String name, String key,
            java.util.function.Function<T, Response> onSuccess) {
        return operation
                .orTimeout(OPERATION_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .handle((value, error) -> {
                    if (error == null) {
                        return onSuccess.apply(value);
                    }
                    Throwable cause = unwrap(error);
                    if (cause instanceof TimeoutException) {
                        logger.warn("{} timeout for key {}", name, key);
                        return Response.error(name + " timeout");
                    }
                    logger.warn("{} failed for key {}: {}", name, key, cause.getMessage());
                    return Response.error(name + " failed: " + cause.getMessage());
                });
    }

    private CompletableFuture<Response> handleGet(Command command) {
        if (command.getKey() == null) {
            return done(Response.error("Key required for GET"));
        }
        if (isDistributed()) {
            return replicated(replicationManager.read(command.getKey(), readConsistency), "Read", command.getKey(),
                    result -> result.filter(r -> r.getValue() != null)
                            .map(r -> Response.ok(r.getValue(), r.getTimestamp(), r.getExpiresAt()))
                            .orElseGet(Response::notFound));
        }
        Optional<KeyValuePair> result = store.get(command.getKey());
        if (result.isPresent()) {
            KeyValuePair kv = result.get();
            return done(Response.ok(kv.getValueUnsafe(), kv.getTimestamp(), kv.getExpiresAt()));
        }
        return done(Response.notFound());
    }

    private CompletableFuture<Response> handleExists(Command command) {
        if (command.getKey() == null) {
            return done(Response.error("Key required for EXISTS"));
        }
        if (isDistributed()) {
            return replicated(replicationManager.read(command.getKey(), readConsistency), "Read", command.getKey(),
                    result -> Response.exists(result.filter(r -> r.getValue() != null).isPresent()));
        }
        return done(Response.exists(store.exists(command.getKey())));
    }

    /**
     * Handle internal GET command from replication (local read only).
     * SECURITY: Only authorized cluster nodes can use this command.
     */
    private Response handleGetInternal(Command command) {
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

    private CompletableFuture<Response> handlePut(Command command) {
        if (command.getKey() == null) {
            return done(Response.error("Key required for PUT"));
        }
        long timestamp = nextTimestamp();
        long expiresAt = command.getExpiresAt();

        if (isDistributed()) {
            return replicated(replicationManager.write(command.getKey(), command.getValueUnsafe(), timestamp,
                    expiresAt, writeConsistency), "Write", command.getKey(), ok -> Response.ok());
        }
        store.put(command.getKey(), command.getValueUnsafe());
        return done(Response.ok());
    }

    /**
     * Handle internal PUT command from replication (no further replication).
     * Uses timestamp-aware putIfNewer to maintain conflict resolution semantics.
     * SECURITY: Only authorized cluster nodes can use this command.
     */
    private Response handlePutInternal(Command command) {
        if (command.getKey() == null) {
            return Response.error("Key required for PUT");
        }
        if (command.getTimestamp() == 0) {
            return Response.error("Timestamp required for internal PUT");
        }
        boolean written = store.putIfNewer(
                command.getKey(),
                command.getValueUnsafe(),
                command.getTimestamp(),
                command.getExpiresAt());
        logger.trace("PUT_INTERNAL {}: key={}, timestamp={}",
                written ? "accepted" : "rejected (stale)", command.getKey(), command.getTimestamp());
        return Response.ok();
    }

    private CompletableFuture<Response> handleDelete(Command command) {
        if (command.getKey() == null) {
            return done(Response.error("Key required for DELETE"));
        }
        // Tombstone timestamp must be newer than any existing entry
        Optional<KeyValuePair> existing = store.getRaw(command.getKey());
        long timestamp = nextTimestamp();
        if (existing.isPresent()) {
            timestamp = Math.max(timestamp, existing.get().getTimestamp() + 1);
        }

        if (isDistributed()) {
            return replicated(replicationManager.delete(command.getKey(), timestamp, 0, deleteConsistency),
                    "Delete", command.getKey(), ok -> Response.ok());
        }
        store.putIfNewer(command.getKey(), null, timestamp, 0);
        return done(Response.ok());
    }

    /**
     * Handle internal DELETE command from replication (no further replication).
     * Uses tombstone with timestamp to prevent resurrection of deleted values.
     * SECURITY: Only authorized cluster nodes can use this command.
     */
    private Response handleDeleteInternal(Command command) {
        if (command.getKey() == null) {
            return Response.error("Key required for DELETE");
        }
        if (command.getTimestamp() == 0) {
            return Response.error("Timestamp required for internal DELETE");
        }
        // null value = tombstone, so stale replicas cannot resurrect the deleted value
        boolean written = store.putIfNewer(
                command.getKey(),
                null,
                command.getTimestamp(),
                command.getExpiresAt());
        logger.trace("DELETE_INTERNAL tombstone {}: key={}, timestamp={}",
                written ? "written" : "rejected (stale)", command.getKey(), command.getTimestamp());
        return Response.ok();
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
