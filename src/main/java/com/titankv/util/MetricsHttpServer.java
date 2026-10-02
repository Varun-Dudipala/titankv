package com.titankv.util;

import com.titankv.cluster.ClusterManager;
import com.titankv.cluster.Node;
import com.titankv.core.InMemoryStore;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Simple HTTP server for metrics export.
 * Exposes Prometheus-compatible metrics endpoint and health checks.
 * 
 * Endpoints:
 * - GET /metrics - Prometheus metrics format
 * - GET /health - Health check (returns 200 if healthy)
 * - GET /ready - Readiness check (returns 200 if ready to serve traffic)
 * - GET /status - JSON status including cluster info
 */
public class MetricsHttpServer {

    private static final Logger logger = LoggerFactory.getLogger(MetricsHttpServer.class);

    private static final String CRLF = "\r\n";

    private final int port;
    private final MetricsCollector metrics;
    private final ClusterManager clusterManager;
    private final InMemoryStore store;
    private final AtomicBoolean running;
    private final ExecutorService executor;
    private ServerSocket serverSocket;
    private Thread acceptThread;

    /**
     * Create a metrics HTTP server on a specific port.
     */
    public MetricsHttpServer(int port, MetricsCollector metrics, ClusterManager clusterManager, InMemoryStore store) {
        this.port = port;
        this.metrics = metrics;
        this.clusterManager = clusterManager;
        this.store = store;
        this.running = new AtomicBoolean(false);
        this.executor = Executors.newFixedThreadPool(2, r -> {
            Thread t = new Thread(r, "metrics-http");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Start the HTTP server.
     */
    public void start() throws IOException {
        if (running.getAndSet(true)) {
            return;
        }

        serverSocket = new ServerSocket();
        serverSocket.setReuseAddress(true);
        serverSocket.bind(new InetSocketAddress(port));
        serverSocket.setSoTimeout(1000); // 1 second timeout for accept

        acceptThread = new Thread(this::acceptLoop, "metrics-http-accept");
        acceptThread.setDaemon(true);
        acceptThread.start();

        logger.info("Metrics HTTP server started on port {}", port);
    }

    /**
     * Stop the HTTP server.
     */
    public void stop() {
        if (!running.getAndSet(false)) {
            return;
        }

        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException e) {
            logger.debug("Error closing server socket: {}", e.getMessage());
        }

        if (acceptThread != null) {
            try {
                acceptThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        logger.info("Metrics HTTP server stopped");
    }

    private void acceptLoop() {
        while (running.get()) {
            try {
                Socket client = serverSocket.accept();
                client.setSoTimeout(5000);
                executor.submit(() -> handleRequest(client));
            } catch (SocketTimeoutException e) {
                // Expected, continue
            } catch (IOException e) {
                if (running.get()) {
                    logger.error("Error accepting connection: {}", e.getMessage());
                }
            }
        }
    }

    private void handleRequest(Socket client) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(client.getInputStream()));
             OutputStream out = client.getOutputStream()) {

            // Read request line
            String requestLine = reader.readLine();
            if (requestLine == null || requestLine.isEmpty()) {
                return;
            }

            // Parse request
            String[] parts = requestLine.split(" ");
            if (parts.length < 2) {
                sendError(out, 400, "Bad Request");
                return;
            }

            String method = parts[0];
            String path = parts[1];

            // Only support GET
            if (!"GET".equalsIgnoreCase(method)) {
                sendError(out, 405, "Method Not Allowed");
                return;
            }

            // Consume headers
            String line;
            while ((line = reader.readLine()) != null && !line.isEmpty()) {
                // Discard headers
            }

            // Route request
            switch (path) {
                case "/metrics":
                    handleMetrics(out);
                    break;
                case "/health":
                    handleHealth(out);
                    break;
                case "/ready":
                    handleReady(out);
                    break;
                case "/status":
                    handleStatus(out);
                    break;
                default:
                    sendError(out, 404, "Not Found");
            }

        } catch (IOException e) {
            logger.debug("Error handling request: {}", e.getMessage());
        } finally {
            try {
                client.close();
            } catch (IOException e) {
                // Ignore
            }
        }
    }

    /**
     * Prometheus text format: counters end in _total, timers become _seconds_count/_sum/_max, and
     * every metric family gets one # HELP and # TYPE line.
     */
    private void handleMetrics(OutputStream out) throws IOException {
        StringBuilder sb = new StringBuilder();
        java.util.Map<String, java.util.List<Meter>> families = new java.util.TreeMap<>();
        for (Meter meter : metrics.getRegistry().getMeters()) {
            families.computeIfAbsent(meter.getId().getName(), n -> new java.util.ArrayList<>()).add(meter);
        }
        for (java.util.List<Meter> family : families.values()) {
            Meter first = family.get(0);
            String name = first.getId().getName().replace('.', '_');
            String help = first.getId().getDescription() != null ? first.getId().getDescription() : name;
            if (first instanceof Counter || first instanceof FunctionCounter) {
                header(sb, name + "_total", help, "counter");
                for (Meter meter : family) {
                    double count = meter instanceof Counter ? ((Counter) meter).count()
                            : ((FunctionCounter) meter).count();
                    sample(sb, name + "_total", formatTags(meter), count);
                }
            } else if (first instanceof Timer) {
                header(sb, name + "_seconds", help, "summary");
                for (Meter meter : family) {
                    Timer timer = (Timer) meter;
                    String tags = formatTags(meter);
                    sample(sb, name + "_seconds_count", tags, timer.count());
                    sample(sb, name + "_seconds_sum", tags, timer.totalTime(TimeUnit.SECONDS));
                }
                header(sb, name + "_seconds_max", help + " (recent maximum)", "gauge");
                for (Meter meter : family) {
                    sample(sb, name + "_seconds_max", formatTags(meter), ((Timer) meter).max(TimeUnit.SECONDS));
                }
            } else {
                header(sb, name, help, "gauge");
                for (Meter meter : family) {
                    for (io.micrometer.core.instrument.Measurement m : meter.measure()) {
                        sample(sb, name, formatTags(meter), m.getValue());
                    }
                }
            }
        }

        header(sb, "titankv_info", "TitanKV server info", "gauge");
        sb.append("titankv_info{version=\"1.0.0\"} 1\n");
        if (clusterManager != null) {
            header(sb, "titankv_cluster_nodes", "Members of the cluster, including down ones", "gauge");
            sample(sb, "titankv_cluster_nodes", "", clusterManager.getNodeCount());
            header(sb, "titankv_cluster_alive_nodes", "Members currently considered up", "gauge");
            sample(sb, "titankv_cluster_alive_nodes", "", clusterManager.getAliveNodeCount());
        }
        if (store != null) {
            header(sb, "titankv_store_keys", "Live keys stored on this node", "gauge");
            sample(sb, "titankv_store_keys", "", store.size());
            header(sb, "titankv_store_keys_raw", "Stored entries including tombstones and expired entries", "gauge");
            sample(sb, "titankv_store_keys_raw", "", store.rawSize());
        }

        sendResponse(out, 200, "text/plain; version=0.0.4; charset=utf-8", sb.toString());
    }

    private static void header(StringBuilder sb, String name, String help, String type) {
        sb.append("# HELP ").append(name).append(' ').append(help.replace("\n", " ")).append('\n');
        sb.append("# TYPE ").append(name).append(' ').append(type).append('\n');
    }

    private static void sample(StringBuilder sb, String name, String tags, double value) {
        sb.append(name);
        if (!tags.isEmpty()) {
            sb.append('{').append(tags).append('}');
        }
        sb.append(' ');
        if (value == Math.rint(value) && !Double.isInfinite(value) && Math.abs(value) < 1e15) {
            sb.append((long) value);
        } else {
            sb.append(value);
        }
        sb.append('\n');
    }

    private String formatTags(Meter meter) {
        StringBuilder sb = new StringBuilder();
        meter.getId().getTags().forEach(tag -> {
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append(tag.getKey()).append("=\"").append(escapeValue(tag.getValue())).append("\"");
        });
        return sb.toString();
    }

    private String escapeValue(String value) {
        return value.replace("\\", "\\\\")
                    .replace("\"", "\\\"")
                    .replace("\n", "\\n");
    }

    private void handleHealth(OutputStream out) throws IOException {
        // Basic health check - server is running
        sendResponse(out, 200, "application/json", "{\"status\":\"healthy\"}");
    }

    private void handleReady(OutputStream out) throws IOException {
        if (clusterManager == null || clusterManager.isReady()) {
            sendResponse(out, 200, "application/json", "{\"status\":\"ready\"}");
        } else {
            sendResponse(out, 503, "application/json",
                    "{\"status\":\"not ready\",\"reason\":\"joining cluster\"}");
        }
    }

    private void handleStatus(OutputStream out) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        sb.append("  \"version\": \"1.0.0\",\n");
        sb.append("  \"status\": \"running\",\n");

        // Metrics
        sb.append("  \"metrics\": {\n");
        sb.append("    \"total_gets\": ").append(metrics.getTotalGetOps()).append(",\n");
        sb.append("    \"total_puts\": ").append(metrics.getTotalPutOps()).append(",\n");
        sb.append("    \"total_deletes\": ").append(metrics.getTotalDeleteOps()).append(",\n");
        sb.append("    \"total_errors\": ").append(metrics.getTotalErrors()).append(",\n");
        sb.append("    \"active_connections\": ").append(metrics.getActiveConnections()).append(",\n");
        sb.append("    \"hit_rate\": ").append(String.format("%.4f", metrics.getHitRate())).append(",\n");
        sb.append("    \"get_mean_latency_ms\": ").append(String.format("%.3f", metrics.getGetMeanLatencyMs())).append(",\n");
        sb.append("    \"put_mean_latency_ms\": ").append(String.format("%.3f", metrics.getPutMeanLatencyMs())).append("\n");
        sb.append("  },\n");

        // Cluster info
        sb.append("  \"cluster\": {\n");
        if (clusterManager != null) {
            sb.append("    \"running\": ").append(clusterManager.isRunning()).append(",\n");
            sb.append("    \"ready\": ").append(clusterManager.isReady()).append(",\n");
            sb.append("    \"total_nodes\": ").append(clusterManager.getNodeCount()).append(",\n");
            sb.append("    \"alive_nodes\": ").append(clusterManager.getAliveNodeCount()).append(",\n");
            sb.append("    \"local_node\": \"").append(jsonEscape(clusterManager.getLocalNode().getId())).append("\",\n");
            sb.append("    \"nodes\": [\n");
            boolean first = true;
            for (Node node : clusterManager.getAllNodes()) {
                if (!first) {
                    sb.append(",\n");
                }
                first = false;
                sb.append("      {\"id\": \"").append(jsonEscape(node.getId()))
                  .append("\", \"status\": \"").append(node.getStatus())
                  .append("\", \"address\": \"").append(jsonEscape(node.getAddress())).append("\"}");
            }
            sb.append("\n    ]\n");
        } else {
            sb.append("    \"running\": false,\n");
            sb.append("    \"total_nodes\": 1,\n");
            sb.append("    \"alive_nodes\": 1\n");
        }
        sb.append("  },\n");

        // Store info
        sb.append("  \"store\": {\n");
        if (store != null) {
            sb.append("    \"keys\": ").append(store.size()).append(",\n");
            sb.append("    \"keys_raw\": ").append(store.rawSize()).append("\n");
        } else {
            sb.append("    \"keys\": 0,\n");
            sb.append("    \"keys_raw\": 0\n");
        }
        sb.append("  }\n");

        sb.append("}");

        sendResponse(out, 200, "application/json", sb.toString());
    }

    private static String jsonEscape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private void sendResponse(OutputStream out, int statusCode, String contentType, String body) throws IOException {
        byte[] bodyBytes = body.getBytes(StandardCharsets.UTF_8);
        String statusText = getStatusText(statusCode);

        StringBuilder headers = new StringBuilder();
        headers.append("HTTP/1.1 ").append(statusCode).append(" ").append(statusText).append(CRLF);
        headers.append("Content-Type: ").append(contentType).append(CRLF);
        headers.append("Content-Length: ").append(bodyBytes.length).append(CRLF);
        headers.append("Connection: close").append(CRLF);
        headers.append(CRLF);

        out.write(headers.toString().getBytes(StandardCharsets.US_ASCII));
        out.write(bodyBytes);
        out.flush();
    }

    private void sendError(OutputStream out, int statusCode, String message) throws IOException {
        sendResponse(out, statusCode, "text/plain", message);
    }

    private String getStatusText(int code) {
        switch (code) {
            case 200: return "OK";
            case 400: return "Bad Request";
            case 404: return "Not Found";
            case 405: return "Method Not Allowed";
            case 503: return "Service Unavailable";
            default: return "Unknown";
        }
    }

    /**
     * Get the port this server is listening on.
     */
    public int getPort() {
        return port;
    }

    /**
     * Check if the server is running.
     */
    public boolean isRunning() {
        return running.get();
    }
}
