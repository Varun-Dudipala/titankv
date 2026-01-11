package com.titankv.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

/**
 * UDP-based gossip protocol for cluster membership.
 * Implements basic membership gossip with heartbeats.
 */
public class GossipProtocol {

    private static final Logger logger = LoggerFactory.getLogger(GossipProtocol.class);

    // Gossip message types
    private static final byte MSG_JOIN = 0x01;
    private static final byte MSG_LEAVE = 0x02;
    private static final byte MSG_HEARTBEAT = 0x03;
    private static final byte MSG_MEMBERS = 0x04;
    private static final byte MSG_ACK = 0x05;

    private static final int GOSSIP_PORT_OFFSET = 1000;
    private static final int HEARTBEAT_INTERVAL_MS = 1000;
    private static final int GOSSIP_FANOUT = 3;  // Number of nodes to gossip to each round
    private static final int MAX_PACKET_SIZE = 65507;

    private final Node localNode;
    private final ClusterManager clusterManager;
    private final int gossipPort;

    private DatagramSocket socket;
    private ExecutorService executor;
    private ScheduledExecutorService scheduler;
    private volatile boolean running;

    /**
     * Create a gossip protocol instance.
     *
     * @param localNode      the local node
     * @param clusterManager the cluster manager
     */
    public GossipProtocol(Node localNode, ClusterManager clusterManager) {
        this.localNode = localNode;
        this.clusterManager = clusterManager;
        this.gossipPort = localNode.getPort() + GOSSIP_PORT_OFFSET;
        this.running = false;
    }

    /**
     * Start the gossip protocol.
     */
    public void start() {
        if (running) {
            return;
        }

        try {
            socket = new DatagramSocket(gossipPort);
            socket.setSoTimeout(100);
        } catch (SocketException e) {
            logger.error("Failed to start gossip on port {}: {}", gossipPort, e.getMessage());
            return;
        }

        running = true;

        executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "gossip-receiver");
            t.setDaemon(true);
            return t;
        });
        executor.submit(this::receiveLoop);

        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "gossip-sender");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::gossipRound,
            HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);

        logger.info("Gossip protocol started on port {}", gossipPort);
    }

    /**
     * Stop the gossip protocol.
     */
    public void stop() {
        if (!running) {
            return;
        }

        running = false;

        // Notify cluster we're leaving
        broadcastLeave();

        if (scheduler != null) {
            scheduler.shutdown();
        }
        if (executor != null) {
            executor.shutdown();
        }
        if (socket != null) {
            socket.close();
        }

        logger.info("Gossip protocol stopped");
    }

    /**
     * Send a join message to a node.
     */
    public void sendJoin(Node target) {
        ByteBuffer buffer = ByteBuffer.allocate(256);
        buffer.put(MSG_JOIN);
        writeString(buffer, localNode.getId());
        writeString(buffer, localNode.getHost());
        buffer.putInt(localNode.getPort());

        send(buffer, target);
        logger.debug("Sent JOIN to {}", target.getId());
    }

    /**
     * Broadcast leave message to all nodes.
     */
    private void broadcastLeave() {
        ByteBuffer buffer = ByteBuffer.allocate(256);
        buffer.put(MSG_LEAVE);
        writeString(buffer, localNode.getId());

        for (Node node : clusterManager.getAllNodes()) {
            if (!node.equals(localNode)) {
                send(buffer.duplicate(), node);
            }
        }
    }

    /**
     * Perform a gossip round - send heartbeats and membership info.
     */
    private void gossipRound() {
        if (!running) {
            return;
        }

        Collection<Node> allNodes = clusterManager.getAllNodes();
        List<Node> targets = selectGossipTargets(allNodes);

        // Send heartbeat to selected targets
        ByteBuffer heartbeat = createHeartbeat();
        for (Node target : targets) {
            send(heartbeat.duplicate(), target);
        }

        // Occasionally send full membership list
        if (ThreadLocalRandom.current().nextDouble() < 0.1) {
            ByteBuffer members = createMembersList();
            for (Node target : targets) {
                send(members.duplicate(), target);
            }
        }
    }

    /**
     * Select random nodes to gossip to.
     */
    private List<Node> selectGossipTargets(Collection<Node> allNodes) {
        List<Node> candidates = new ArrayList<>();
        for (Node node : allNodes) {
            if (!node.equals(localNode) && node.isAvailable()) {
                candidates.add(node);
            }
        }

        if (candidates.isEmpty()) {
            return Collections.emptyList();
        }

        Collections.shuffle(candidates);
        return candidates.subList(0, Math.min(GOSSIP_FANOUT, candidates.size()));
    }

    /**
     * Create a heartbeat message.
     */
    private ByteBuffer createHeartbeat() {
        ByteBuffer buffer = ByteBuffer.allocate(256);
        buffer.put(MSG_HEARTBEAT);
        writeString(buffer, localNode.getId());
        buffer.putLong(System.currentTimeMillis());
        return buffer;
    }

    /**
     * Create a members list message.
     */
    private ByteBuffer createMembersList() {
        Collection<Node> nodes = clusterManager.getAllNodes();
        ByteBuffer buffer = ByteBuffer.allocate(Math.min(MAX_PACKET_SIZE, 256 + nodes.size() * 128));
        buffer.put(MSG_MEMBERS);
        buffer.putInt(nodes.size());

        for (Node node : nodes) {
            writeString(buffer, node.getId());
            writeString(buffer, node.getHost());
            buffer.putInt(node.getPort());
            buffer.put((byte) node.getStatus().ordinal());
        }

        return buffer;
    }

    /**
     * Receive loop for incoming gossip messages.
     */
    private void receiveLoop() {
        byte[] receiveBuffer = new byte[MAX_PACKET_SIZE];
        DatagramPacket packet = new DatagramPacket(receiveBuffer, receiveBuffer.length);

        while (running) {
            try {
                socket.receive(packet);
                ByteBuffer buffer = ByteBuffer.wrap(packet.getData(), 0, packet.getLength());
                handleMessage(buffer, packet.getAddress(), packet.getPort());
            } catch (SocketTimeoutException e) {
                // Expected, continue
            } catch (IOException e) {
                if (running) {
                    logger.error("Error receiving gossip: {}", e.getMessage());
                }
            }
        }
    }

    /**
     * Handle an incoming gossip message.
     */
    private void handleMessage(ByteBuffer buffer, InetAddress senderAddress, int senderPort) {
        byte msgType = buffer.get();

        switch (msgType) {
            case MSG_JOIN:
                handleJoin(buffer);
                break;
            case MSG_LEAVE:
                handleLeave(buffer);
                break;
            case MSG_HEARTBEAT:
                handleHeartbeat(buffer);
                break;
            case MSG_MEMBERS:
                handleMembers(buffer);
                break;
            case MSG_ACK:
                // Acknowledgment received
                break;
            default:
                logger.warn("Unknown gossip message type: {}", msgType);
        }
    }

    private void handleJoin(ByteBuffer buffer) {
        String nodeId = readString(buffer);
        String host = readString(buffer);
        int port = buffer.getInt();

        Node node = new Node(nodeId, host, port);
        clusterManager.addNode(node);

        // Send current membership to the joining node
        ByteBuffer members = createMembersList();
        send(members, node);
    }

    private void handleLeave(ByteBuffer buffer) {
        String nodeId = readString(buffer);
        Node node = clusterManager.getNode(nodeId);
        if (node != null) {
            clusterManager.removeNode(node);
        }
    }

    private void handleHeartbeat(ByteBuffer buffer) {
        String nodeId = readString(buffer);
        long timestamp = buffer.getLong();
        clusterManager.updateHeartbeat(nodeId);
    }

    private void handleMembers(ByteBuffer buffer) {
        int count = buffer.getInt();

        for (int i = 0; i < count; i++) {
            String nodeId = readString(buffer);
            String host = readString(buffer);
            int port = buffer.getInt();
            byte statusOrdinal = buffer.get();

            if (!nodeId.equals(localNode.getId())) {
                Node existing = clusterManager.getNode(nodeId);
                if (existing == null) {
                    Node node = new Node(nodeId, host, port);
                    node.setStatus(Node.Status.values()[statusOrdinal]);
                    clusterManager.addNode(node);
                }
            }
        }
    }

    /**
     * Send a message to a node.
     */
    private void send(ByteBuffer buffer, Node target) {
        try {
            buffer.flip();
            byte[] data = new byte[buffer.remaining()];
            buffer.get(data);

            int targetGossipPort = target.getPort() + GOSSIP_PORT_OFFSET;
            InetAddress address = InetAddress.getByName(target.getHost());
            DatagramPacket packet = new DatagramPacket(data, data.length, address, targetGossipPort);
            socket.send(packet);
        } catch (IOException e) {
            logger.debug("Failed to send gossip to {}: {}", target.getId(), e.getMessage());
        }
    }

    private void writeString(ByteBuffer buffer, String str) {
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        buffer.putShort((short) bytes.length);
        buffer.put(bytes);
    }

    private String readString(ByteBuffer buffer) {
        short length = buffer.getShort();
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Get the gossip port.
     */
    public int getGossipPort() {
        return gossipPort;
    }
}
