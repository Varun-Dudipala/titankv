package com.titankv;

import com.titankv.cluster.ClusterManager;
import com.titankv.cluster.DataRebalancer;
import com.titankv.cluster.HintedHandoff;
import com.titankv.cluster.Node;
import com.titankv.consistency.ReplicationManager;
import com.titankv.core.InMemoryStore;
import com.titankv.core.KVStore;
import com.titankv.network.TcpServer;
import com.titankv.util.MetricsCollector;
import com.titankv.util.MetricsHttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
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
    private final ReplicationManager replicationManager;
    private final CountDownLatch shutdownLatch;
    private final String seedNodes;
    private MetricsHttpServer metricsHttpServer;
    private HintedHandoff hintedHandoff;
    private DataRebalancer dataRebalancer;

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
        this.shutdownLatch = new CountDownLatch(1);
        this.nodeId = generateNodeId(port);

        Node localNode = new Node(this.nodeId, "localhost", port);
        this.clusterManager = new ClusterManager(localNode);
        this.replicationManager = new ReplicationManager(clusterManager);
        this.tcpServer = new TcpServer(port, store, metrics, replicationManager, clusterManager);
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
        this.replicationManager = new ReplicationManager(clusterManager);
        this.tcpServer = new TcpServer(port, store, metrics, replicationManager, clusterManager);
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
        this.shutdownLatch = new CountDownLatch(1);

        Node localNode = new Node(this.nodeId, "localhost", port);
        this.clusterManager = new ClusterManager(localNode);
        this.replicationManager = new ReplicationManager(clusterManager);
        this.tcpServer = new TcpServer(port, store, metrics, replicationManager, clusterManager);
    }

    private static String generateNodeId(int port) {
        try {
            String host = InetAddress.getLocalHost().getHostName();
            return host + ":" + port;
        } catch (UnknownHostException e) {
            logger.debug("Unable to determine hostname: {}", e.getMessage());
            return "localhost:" + port;
        }
    }

    private String getHostFromId(String nodeId) {
        if (nodeId.contains(":")) {
            return nodeId.substring(0, nodeId.lastIndexOf(':'));
        }
        // If nodeId doesn't contain a colon, it's just a label - default to localhost
        return "localhost";
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

        // Initialize and start metrics HTTP server
        if (store instanceof InMemoryStore) {
            metricsHttpServer = new MetricsHttpServer(metrics, clusterManager, (InMemoryStore) store);
            try {
                metricsHttpServer.start();
                logger.info("Metrics HTTP server started on port {}", metricsHttpServer.getPort());
            } catch (IOException e) {
                logger.warn("Failed to start metrics HTTP server: {}", e.getMessage());
            }

            // Initialize hinted handoff
            Path dataDir = Path.of(System.getProperty("titankv.data.dir", 
                System.getenv().getOrDefault("TITANKV_DATA_DIR", "data")));
            hintedHandoff = new HintedHandoff(clusterManager, dataDir);
            hintedHandoff.start();
            logger.info("Hinted handoff started");

            // Initialize data rebalancer
            dataRebalancer = new DataRebalancer(clusterManager, (InMemoryStore) store, 
                replicationManager.getReplicationFactor());
            dataRebalancer.start();
            logger.info("Data rebalancer started");
        }

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

        // Stop metrics HTTP server
        if (metricsHttpServer != null) {
            metricsHttpServer.stop();
        }

        // Stop data rebalancer
        if (dataRebalancer != null) {
            dataRebalancer.stop();
        }

        // Stop hinted handoff
        if (hintedHandoff != null) {
            hintedHandoff.stop();
        }

        // Stop cluster manager (graceful leave)
        clusterManager.stop();

        // Stop replication manager
        replicationManager.shutdown();

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
        String clusterSecret = null;
        boolean devMode = false;

        // Parse command line arguments
        try {
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--port":
                    case "-p":
                        if (i + 1 >= args.length) {
                            exitWithError("--port requires a value");
                        }
                        try {
                            port = Integer.parseInt(args[++i]);
                            if (port <= 0 || port > 65535) {
                                exitWithError("Port must be between 1 and 65535");
                            }
                        } catch (NumberFormatException e) {
                            exitWithError("Invalid port number: " + args[i]);
                        }
                        break;
                    case "--node-id":
                    case "-n":
                        if (i + 1 >= args.length) {
                            exitWithError("--node-id requires a value");
                        }
                        nodeId = args[++i];
                        break;
                    case "--seeds":
                    case "-s":
                        if (i + 1 >= args.length) {
                            exitWithError("--seeds requires a value");
                        }
                        seedNodes = args[++i];
                        break;
                    case "--cluster-secret":
                        if (i + 1 >= args.length) {
                            exitWithError("--cluster-secret requires a value");
                        }
                        clusterSecret = args[++i];
                        break;
                    case "--dev":
                        devMode = true;
                        break;
                    case "--help":
                    case "-h":
                        printHelp();
                        return;
                    case "--version":
                    case "-v":
                        System.out.println("TitanKV Server v" + VERSION);
                        return;
                    default:
                        exitWithError("Unknown option: " + args[i]);
                }
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            exitWithError("Missing argument value");
        }

        // Set system properties for cluster authentication
        if (clusterSecret != null) {
            System.setProperty("titankv.cluster.secret", clusterSecret);
        }
        if (devMode) {
            System.setProperty("titankv.dev.mode", "true");
        }

        printBanner();

        // Ensure logs directory exists
        ensureLogsDirectory();

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

    private static void ensureLogsDirectory() {
        java.io.File logsDir = new java.io.File("logs");
        if (!logsDir.exists()) {
            if (logsDir.mkdir()) {
                logger.info("Created logs directory");
            } else {
                logger.warn("Failed to create logs directory, file logging may not work");
            }
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

    private static void exitWithError(String message) {
        System.err.println("Error: " + message);
        System.err.println("Use --help for usage information");
        System.exit(1);
    }

    private static void printHelp() {
        System.out.println("TitanKV Server - Distributed Key-Value Store");
        System.out.println();
        System.out.println("Usage: titankv [options]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  -p, --port <port>            Port to listen on (default: 9001)");
        System.out.println("  -n, --node-id <id>           Node identifier (default: hostname:port)");
        System.out.println("  -s, --seeds <hosts>          Comma-separated seed node addresses");
        System.out.println("      --cluster-secret <secret> Cluster authentication secret (required for production)");
        System.out.println("      --dev                     Enable development mode (disables auth requirement)");
        System.out.println("  -h, --help                   Show this help message");
        System.out.println("  -v, --version                Show version");
        System.out.println();
        System.out.println("Security:");
        System.out.println("  By default, TitanKV requires a cluster secret for production deployments.");
        System.out.println("  Set --cluster-secret or TITANKV_CLUSTER_SECRET environment variable.");
        System.out.println("  Use --dev flag ONLY for local development (disables authentication).");
        System.out.println("  To require client authentication, set TITANKV_CLIENT_TOKEN.");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  # Single node (development)");
        System.out.println("  titankv --port 9001 --dev");
        System.out.println();
        System.out.println("  # Production cluster with authentication");
        System.out.println("  titankv --port 9001 --cluster-secret mySecretKey");
        System.out.println("  titankv --port 9002 --cluster-secret mySecretKey --seeds localhost:9001");
        System.out.println();
        System.out.println("  # Using environment variable");
        System.out.println("  export TITANKV_CLUSTER_SECRET=mySecretKey");
        System.out.println("  titankv --port 9001");
        System.out.println();
    }
}
