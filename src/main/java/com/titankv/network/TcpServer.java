package com.titankv.network;

import com.titankv.core.KVStore;
import com.titankv.util.MetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * NIO-based TCP server for TitanKV.
 * Uses a single-threaded selector event loop for accepting connections
 * and handling I/O events.
 */
public class TcpServer {

    private static final Logger logger = LoggerFactory.getLogger(TcpServer.class);

    private final int port;
    private final KVStore store;
    private final MetricsCollector metrics;
    private final AtomicBoolean running;
    private final Map<SocketChannel, ConnectionHandler> connections;

    private Selector selector;
    private ServerSocketChannel serverChannel;
    private Thread serverThread;

    /**
     * Create a new TCP server.
     *
     * @param port    the port to listen on
     * @param store   the key-value store to use
     * @param metrics the metrics collector
     */
    public TcpServer(int port, KVStore store, MetricsCollector metrics) {
        this.port = port;
        this.store = store;
        this.metrics = metrics;
        this.running = new AtomicBoolean(false);
        this.connections = new ConcurrentHashMap<>();
    }

    /**
     * Start the server.
     *
     * @throws IOException if the server cannot be started
     */
    public void start() throws IOException {
        if (running.getAndSet(true)) {
            throw new IllegalStateException("Server already running");
        }

        selector = Selector.open();
        serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.socket().setReuseAddress(true);
        serverChannel.bind(new InetSocketAddress(port));
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        serverThread = new Thread(this::eventLoop, "titankv-server-" + port);
        serverThread.start();

        logger.info("TitanKV server started on port {}", port);
    }

    /**
     * Start the server and block until it's stopped.
     *
     * @throws IOException if the server cannot be started
     */
    public void startAndBlock() throws IOException {
        if (running.getAndSet(true)) {
            throw new IllegalStateException("Server already running");
        }

        selector = Selector.open();
        serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.socket().setReuseAddress(true);
        serverChannel.bind(new InetSocketAddress(port));
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        logger.info("TitanKV server started on port {}", port);
        eventLoop();
    }

    private void eventLoop() {
        while (running.get()) {
            try {
                int ready = selector.select(1000); // 1 second timeout for clean shutdown

                if (ready == 0) {
                    continue;
                }

                Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
                while (keys.hasNext()) {
                    SelectionKey key = keys.next();
                    keys.remove();

                    if (!key.isValid()) {
                        continue;
                    }

                    try {
                        if (key.isAcceptable()) {
                            accept();
                        }
                        if (key.isReadable()) {
                            read(key);
                        }
                        if (key.isWritable()) {
                            write(key);
                        }
                    } catch (CancelledKeyException e) {
                        // Key was cancelled, ignore
                    }
                }
            } catch (IOException e) {
                if (running.get()) {
                    logger.error("Selector error: {}", e.getMessage());
                }
            }
        }

        cleanup();
    }

    private void accept() throws IOException {
        SocketChannel clientChannel = serverChannel.accept();
        if (clientChannel == null) {
            return;
        }

        clientChannel.configureBlocking(false);
        clientChannel.socket().setTcpNoDelay(true);
        clientChannel.socket().setKeepAlive(true);

        ConnectionHandler handler = new ConnectionHandler(clientChannel, store, metrics);
        connections.put(clientChannel, handler);

        clientChannel.register(selector, SelectionKey.OP_READ, handler);

        logger.debug("Accepted connection from {}", handler.getRemoteAddress());
    }

    private void read(SelectionKey key) {
        ConnectionHandler handler = (ConnectionHandler) key.attachment();
        if (handler == null) {
            key.cancel();
            return;
        }

        if (!handler.handleRead(key)) {
            closeConnection(key, handler);
        }
    }

    private void write(SelectionKey key) {
        ConnectionHandler handler = (ConnectionHandler) key.attachment();
        if (handler == null) {
            key.cancel();
            return;
        }

        if (!handler.handleWrite(key)) {
            closeConnection(key, handler);
        }
    }

    private void closeConnection(SelectionKey key, ConnectionHandler handler) {
        key.cancel();
        SocketChannel channel = (SocketChannel) key.channel();
        connections.remove(channel);
        handler.close();
    }

    /**
     * Stop the server.
     */
    public void stop() {
        if (!running.getAndSet(false)) {
            return;
        }

        logger.info("Stopping TitanKV server on port {}", port);

        // Wake up the selector to exit the event loop
        if (selector != null) {
            selector.wakeup();
        }

        // Wait for the server thread to finish
        if (serverThread != null && serverThread != Thread.currentThread()) {
            try {
                serverThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void cleanup() {
        // Close all connections
        for (ConnectionHandler handler : connections.values()) {
            handler.close();
        }
        connections.clear();

        // Close server channel
        if (serverChannel != null) {
            try {
                serverChannel.close();
            } catch (IOException e) {
                logger.debug("Error closing server channel: {}", e.getMessage());
            }
        }

        // Close selector
        if (selector != null) {
            try {
                selector.close();
            } catch (IOException e) {
                logger.debug("Error closing selector: {}", e.getMessage());
            }
        }

        logger.info("TitanKV server stopped on port {}", port);
    }

    /**
     * Check if the server is running.
     */
    public boolean isRunning() {
        return running.get();
    }

    /**
     * Get the number of active connections.
     */
    public int getConnectionCount() {
        return connections.size();
    }

    /**
     * Get the port this server is listening on.
     */
    public int getPort() {
        return port;
    }
}
