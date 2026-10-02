package com.titankv.cluster;

import com.titankv.TitanKVClient;
import com.titankv.client.ClientConfig;
import com.titankv.core.InMemoryStore;
import com.titankv.core.KeyValuePair;
import com.titankv.util.Env;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * Streams data to a node that joins the cluster: every node sends the new node the keys (and
 * tombstones) the two of them now share, with their original versions. Runs in the background.
 */
public class DataRebalancer {

    private static final Logger logger = LoggerFactory.getLogger(DataRebalancer.class);

    private static final int MAX_CONCURRENT_TRANSFERS = 4;
    private static final long TRANSFER_TIMEOUT_MS = 30_000;

    private final ClusterManager clusterManager;
    private final InMemoryStore store;
    private final int replicationFactor;
    private final ExecutorService transferPool;
    private final Map<String, TitanKVClient> nodeClients;
    private final String authToken;
    private volatile boolean running;


    public DataRebalancer(ClusterManager clusterManager, InMemoryStore store, int replicationFactor) {
        this.clusterManager = clusterManager;
        this.store = store;
        this.replicationFactor = replicationFactor;
        this.nodeClients = new ConcurrentHashMap<>();
        this.authToken = Env.internalToken();
        this.running = false;

        this.transferPool = new ThreadPoolExecutor(
            MAX_CONCURRENT_TRANSFERS,
            MAX_CONCURRENT_TRANSFERS,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(1000),
            r -> {
                Thread t = new Thread(r, "data-rebalancer");
                t.setDaemon(true);
                return t;
            },
            new ThreadPoolExecutor.CallerRunsPolicy()
        );

        // Register for cluster events
        clusterManager.addEventListener(this::handleClusterEvent);
    }

    /**
     * Start the data rebalancer.
     */
    public void start() {
        if (running) {
            return;
        }
        running = true;
        logger.info("Data rebalancer started");
    }

    /**
     * Stop the data rebalancer.
     */
    public void stop() {
        if (!running) {
            return;
        }
        running = false;

        transferPool.shutdown();
        try {
            if (!transferPool.awaitTermination(30, TimeUnit.SECONDS)) {
                transferPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            transferPool.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Close all clients
        for (TitanKVClient client : nodeClients.values()) {
            client.close();
        }
        nodeClients.clear();

        logger.info("Data rebalancer stopped");
    }

    private void handleClusterEvent(ClusterManager.ClusterEvent event) {
        if (!running) {
            return;
        }

        // A new node starts empty: stream it every key it now replicates. A restarted node is
        // repaired by hints and anti-entropy instead, which only transfer what it is missing, and a
        // removed node's keys are re-replicated by anti-entropy.
        if (event.getType() == ClusterManager.ClusterEvent.Type.NODE_JOINED) {
            transferPool.submit(() -> handleNodeJoin(event.getNode()));
        }
    }

    private void handleNodeJoin(Node newNode) {
        if (newNode.equals(clusterManager.getLocalNode())) {
            return; // existing replicas push data to us
        }
        TitanKVClient client = getClient(newNode);
        int transferred = 0;
        int failed = 0;
        // Include tombstones so the new node does not serve values that were deleted
        for (String key : store.keysIncludingTombstones()) {
            if (!sharedWith(newNode, key)) {
                continue;
            }
            Optional<KeyValuePair> entry = store.getRaw(key);
            if (entry.isEmpty()) {
                continue;
            }
            KeyValuePair kv = entry.get();
            try {
                if (kv.isTombstone()) {
                    client.deleteInternal(key, kv.getTimestamp(), kv.getExpiresAt());
                } else {
                    client.putInternal(key, kv.getValueUnsafe(), kv.getTimestamp(), kv.getExpiresAt());
                }
                transferred++;
            } catch (IOException e) {
                failed++; // anti-entropy repairs it later
                logger.debug("Failed to transfer key {} to {}: {}", key, newNode.getId(), e.getMessage());
            }
        }
        if (transferred + failed > 0) {
            logger.info("Transfer to new node {} complete: {} keys sent, {} failed", newNode.getId(), transferred,
                    failed);
        }
    }

    /**
     * Whether both this node and the target replicate the key.
     */
    private boolean sharedWith(Node target, String key) {
        List<Node> owners = clusterManager.getReplicasForKey(key, replicationFactor);
        return owners.contains(target) && owners.contains(clusterManager.getLocalNode());
    }

    private TitanKVClient getClient(Node node) {
        return nodeClients.computeIfAbsent(node.getAddress(), addr -> {
            ClientConfig config = ClientConfig.builder()
                .connectTimeoutMs(5000)
                .readTimeoutMs((int) TRANSFER_TIMEOUT_MS)
                .retryOnFailure(false)
                .circuitBreaker(false)
                .authToken(authToken)
                .build();
            return new TitanKVClient(config, addr);
        });
    }
}
