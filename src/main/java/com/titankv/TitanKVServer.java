package com.titankv;

import com.titankv.cluster.ClusterManager;
import com.titankv.cluster.Node;
import com.titankv.core.InMemoryStore;
import com.titankv.core.KVStore;
import com.titankv.network.TcpServer;
import com.titankv.util.MetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.CountDownLatch;

/**
 * TitanKV Server entry point.
 * Starts a distributed key-value store node with cluster support.
 */
public class TitanKVServer {

    private static final Logger logger = LoggerFactory.getLogger(TitanKVServer.class);

    private static final int DEFAULT_PORT = 9001;
    private static final String VERSION = "1.0.0";

    private final int port;
    private final String nodeId;
    private final KVStore store;
    private final MetricsCollector metrics;
    private final TcpServer tcpServer;
    private final ClusterManager clusterManager;
    private final CountDownLatch shutdownLatch;
    private final String seedNodes;

    /**
     * Create a new TitanKV server.
     *
     * @param port the port to listen on
     */
    public TitanKVServer(int port) {
        this.port = port;
        this.seedNodes = null;
        this.store = new InMemoryStore();
        this.metrics = new MetricsCollector();
        this.tcpServer = new TcpServer(port, store, metrics);
        this.shutdownLatch = new CountDownLatch(1);
        this.nodeId = generateNodeId(port);

        Node localNode = new Node(this.nodeId, "localhost", port);
        this.clusterManager = new ClusterManager(localNode);
    }

    /**
     * Create a new TitanKV server with cluster configuration.
     *
     * @param port      the port to listen on
     * @param nodeId    optional node identifier (defaults to host:port)
     * @param seedNodes comma-separated seed node addresses for cluster join
     */
    public TitanKVServer(int port, String nodeId, String seedNodes) {
        this.port = port;
        this.seedNodes = seedNodes;
        this.store = new InMemoryStore();
        this.metrics = new MetricsCollector();
        this.tcpServer = new TcpServer(port, store, metrics);
        this.shutdownLatch = new CountDownLatch(1);

        // Generate node ID if not provided
        if (nodeId == null || nodeId.isEmpty()) {
            this.nodeId = generateNodeId(port);
        } else {
            this.nodeId = nodeId;
        }

        // Create the local node and cluster manager
        Node localNode = new Node(this.nodeId, getHostFromId(this.nodeId), port);
        this.clusterManager = new ClusterManager(localNode);
    }

    /**
     * Create a server with custom store and metrics.
     *
     * @param port    the port to listen on
     * @param store   the key-value store to use
     * @param metrics the metrics collector to use
     */
    public TitanKVServer(int port, KVStore store, MetricsCollector metrics) {
        this.port = port;
        this.nodeId = "localhost:" + port;
        this.seedNodes = null;
        this.store = store;
        this.metrics = metrics;
        this.tcpServer = new TcpServer(port, store, metrics);
        this.shutdownLatch = new CountDownLatch(1);

        Node localNode = new Node(this.nodeId, "localhost", port);
        this.clusterManager = new ClusterManager(localNode);
    }

    private static String generateNodeId(int port) {
        try {
            String host = InetAddress.getLocalHost().getHostName();
            return host + ":" + port;
        } catch (Exception e) {
            return "localhost:" + port;
        }
    }

    private String getHostFromId(String nodeId) {
        if (nodeId.contains(":")) {
            return nodeId.substring(0, nodeId.lastIndexOf(':'));
        }
        return nodeId;
    }

    /**
     * Start the server.
     */
    public void start() throws IOException {
        logger.info("Starting TitanKV Server v{}", VERSION);
        logger.info("Node ID: {}", nodeId);
        logger.info("Port: {}", port);

        // Register shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown signal received");
            stop();
        }, "titankv-shutdown"));

        // Start TCP server
        tcpServer.start();

        // Start cluster manager with seed nodes
        if (seedNodes != null && !seedNodes.isEmpty()) {
            logger.info("Joining cluster via seeds: {}", seedNodes);
        }
        clusterManager.start(seedNodes);

        logger.info("TitanKV Server started successfully");
    }

    /**
     * Start the server and block until stopped.
     */
    public void startAndBlock() throws IOException, InterruptedException {
        start();
        shutdownLatch.await();
    }

    /**
     * Stop the server.
     */
    public void stop() {
        logger.info("Stopping TitanKV Server");

        // Stop cluster manager first (graceful leave)
        clusterManager.stop();

        // Stop TCP server
        tcpServer.stop();

        // Shutdown store
        if (store instanceof InMemoryStore) {
            ((InMemoryStore) store).shutdown();
        }

        shutdownLatch.countDown();
        logger.info("TitanKV Server stopped");
    }

    /**
     * Check if the server is running.
     */
    public boolean isRunning() {
        return tcpServer.isRunning();
    }

    /**
     * Get the server port.
     */
    public int getPort() {
        return port;
    }

    /**
     * Get the node ID.
     */
    public String getNodeId() {
        return nodeId;
    }

    /**
     * Get the underlying store.
     */
    public KVStore getStore() {
        return store;
    }

    /**
     * Get the metrics collector.
     */
    public MetricsCollector getMetrics() {
        return metrics;
    }

    /**
     * Get the cluster manager.
     */
    public ClusterManager getClusterManager() {
        return clusterManager;
    }

    /**
     * Get the number of active connections.
     */
    public int getConnectionCount() {
        return tcpServer.getConnectionCount();
    }

    /**
     * Get the number of nodes in the cluster.
     */
    public int getClusterSize() {
        return clusterManager.getNodeCount();
    }

    /**
     * Main entry point.
     */
    public static void main(String[] args) {
        int port = DEFAULT_PORT;
        String nodeId = null;
        String seedNodes = null;

        // Parse command line arguments
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port":
                case "-p":
                    if (i + 1 < args.length) {
                        port = Integer.parseInt(args[++i]);
                    }
                    break;
                case "--node-id":
                case "-n":
                    if (i + 1 < args.length) {
                        nodeId = args[++i];
                    }
                    break;
                case "--seeds":
                case "-s":
                    if (i + 1 < args.length) {
                        seedNodes = args[++i];
                    }
                    break;
                case "--help":
                case "-h":
                    printHelp();
                    return;
                case "--version":
                case "-v":
                    System.out.println("TitanKV Server v" + VERSION);
                    return;
            }
        }

        printBanner();

        // Create and start the server with cluster config
        TitanKVServer server = new TitanKVServer(port, nodeId, seedNodes);
        try {
            server.startAndBlock();
        } catch (IOException e) {
            logger.error("Failed to start server: {}", e.getMessage());
            System.exit(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void printBanner() {
        System.out.println();
        System.out.println("  _____ _ _              _  ____     __");
        System.out.println(" |_   _(_) |_ __ _ _ __ | |/ /\\ \\   / /");
        System.out.println("   | | | | __/ _` | '_ \\| ' /  \\ \\ / / ");
        System.out.println("   | | | | || (_| | | | | . \\   \\ V /  ");
        System.out.println("   |_| |_|\\__\\__,_|_| |_|_|\\_\\   \\_/   ");
        System.out.println();
        System.out.println("  Distributed Key-Value Store v" + VERSION);
        System.out.println();
    }

    private static void printHelp() {
        System.out.println("TitanKV Server - Distributed Key-Value Store");
        System.out.println();
        System.out.println("Usage: titankv [options]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  -p, --port <port>       Port to listen on (default: 9001)");
        System.out.println("  -n, --node-id <id>      Node identifier (default: hostname:port)");
        System.out.println("  -s, --seeds <hosts>     Comma-separated seed node addresses");
        System.out.println("  -h, --help              Show this help message");
        System.out.println("  -v, --version           Show version");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  titankv --port 9001");
        System.out.println("  titankv --port 9002 --seeds localhost:9001");
        System.out.println("  titankv --port 9003 --seeds localhost:9001,localhost:9002");
        System.out.println();
    }
}
