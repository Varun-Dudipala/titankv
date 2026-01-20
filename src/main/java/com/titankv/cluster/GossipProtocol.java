package com.titankv.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.net.*;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
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
    private static final int GOSSIP_FANOUT = 3; // Number of nodes to gossip to each round
    private static final int MAX_PACKET_SIZE = 1400; // MTU-safe (< 1500 - headers)
    private static final int MAX_STRING_LENGTH = 1024; // Max length for node ID/host strings
    private static final int MAX_MEMBERS_COUNT = 1000; // Max members in a single message
    private static final String HMAC_ALGORITHM = "HmacSHA256";
    private static final int HMAC_SIZE = 32; // HMAC-SHA256 = 32 bytes
    private static final long MAX_MESSAGE_AGE_MS = 5 * 60 * 1000; // 5 minutes
    private static final long MAX_MESSAGE_FUTURE_MS = 60 * 1000; // 1 minute

    private final Node localNode;
    private final ClusterManager clusterManager;
    private final int gossipPort;
    private final SecretKeySpec hmacKey;

    private DatagramSocket socket;
    private ExecutorService executor;
    private ScheduledExecutorService scheduler;
    private volatile boolean running;
    private final ConcurrentHashMap<String, Long> lastMessageTimestamps = new ConcurrentHashMap<>();

    /**
     * Create a gossip protocol instance without authentication.
     *
     * @param localNode      the local node
     * @param clusterManager the cluster manager
     */
    public GossipProtocol(Node localNode, ClusterManager clusterManager) {
        this(localNode, clusterManager, null);
    }

    /**
     * Create a gossip protocol instance with optional HMAC authentication.
     *
     * @param localNode      the local node
     * @param clusterManager the cluster manager
     * @param clusterSecret  optional cluster secret for HMAC authentication (null
     *                       to disable)
     */
    public GossipProtocol(Node localNode, ClusterManager clusterManager, String clusterSecret) {
        this.localNode = localNode;
        this.clusterManager = clusterManager;

        // Calculate gossip port with overflow protection
        int basePort = localNode.getPort();
        int calculatedPort = basePort + GOSSIP_PORT_OFFSET;
        if (calculatedPort > 65535) {
            calculatedPort = basePort + 100; // Fallback for high port numbers
            if (calculatedPort > 65535) {
                throw new IllegalArgumentException("Cannot compute valid gossip port for base port: " + basePort);
            }
        }
        this.gossipPort = calculatedPort;

        // Initialize HMAC key if secret provided
        if (clusterSecret != null && !clusterSecret.isEmpty()) {
            this.hmacKey = new SecretKeySpec(clusterSecret.getBytes(StandardCharsets.UTF_8), HMAC_ALGORITHM);
        } else {
            this.hmacKey = null;
        }

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
            socket.setSoTimeout(500); // 500ms to reduce CPU idle spin
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
     * Stop the gossip protocol with proper shutdown sequencing.
     */
    public void stop() {
        if (!running) {
            return;
        }

        running = false;

        // Notify cluster we're leaving
        broadcastLeave();

        // Shutdown executors with bounded wait
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(2, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                    logger.warn("Gossip scheduler did not terminate gracefully");
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                    logger.warn("Gossip executor did not terminate gracefully");
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        // Close socket last (after executors have stopped using it)
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
        buffer.putLong(System.currentTimeMillis());
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
        buffer.putLong(System.currentTimeMillis());
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
     * Select random nodes to gossip to using reservoir sampling.
     * O(N) single pass instead of O(N log N) shuffle.
     */
    private List<Node> selectGossipTargets(Collection<Node> allNodes) {
        List<Node> result = new ArrayList<>(GOSSIP_FANOUT);
        int seen = 0;
        ThreadLocalRandom random = ThreadLocalRandom.current();

        for (Node node : allNodes) {
            if (node.equals(localNode) || !node.isAvailable()) {
                continue;
            }
            seen++;
            if (result.size() < GOSSIP_FANOUT) {
                result.add(node);
            } else {
                // Reservoir sampling: replace with probability k/seen
                int j = random.nextInt(seen);
                if (j < GOSSIP_FANOUT) {
                    result.set(j, node);
                }
            }
        }
        return result;
    }

    /**
     * Create a heartbeat message.
     */
    private ByteBuffer createHeartbeat() {
        ByteBuffer buffer = ByteBuffer.allocate(256);
        buffer.put(MSG_HEARTBEAT);
        buffer.putLong(System.currentTimeMillis());
        writeString(buffer, localNode.getId());
        return buffer;
    }

    /**
     * Create a members list message.
     * Includes as many nodes as will fit in the packet, preventing buffer overflow.
     */
    private ByteBuffer createMembersList() {
        Collection<Node> nodes = clusterManager.getAllNodes();
        ByteBuffer buffer = ByteBuffer.allocate(MAX_PACKET_SIZE);
        buffer.put(MSG_MEMBERS);
        buffer.putLong(System.currentTimeMillis());
        writeString(buffer, localNode.getId());

        // Reserve space for count, will write actual count later
        int countPosition = buffer.position();
        buffer.putInt(0);

        int includedCount = 0;
        for (Node node : nodes) {
            // Calculate space needed for this node: length(2) + id + length(2) + host +
            // port(4) + status(1)
            int nodeIdBytes = node.getId().getBytes(StandardCharsets.UTF_8).length;
            int hostBytes = node.getHost().getBytes(StandardCharsets.UTF_8).length;
            int spaceNeeded = 2 + nodeIdBytes + 2 + hostBytes + 4 + 1;

            // Check if we have enough space
            if (buffer.remaining() < spaceNeeded) {
                logger.debug("Members list truncated: {} of {} nodes included", includedCount, nodes.size());
                break;
            }

            if (includedCount >= MAX_MEMBERS_COUNT) {
                logger.debug("Members list truncated at MAX_MEMBERS_COUNT: {}", MAX_MEMBERS_COUNT);
                break;
            }

            writeString(buffer, node.getId());
            writeString(buffer, node.getHost());
            buffer.putInt(node.getPort());
            buffer.put((byte) node.getStatus().ordinal());
            includedCount++;
        }

        // Write actual count at the reserved position
        int currentPosition = buffer.position();
        buffer.position(countPosition);
        buffer.putInt(includedCount);
        buffer.position(currentPosition);

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

                // Verify HMAC if authentication is enabled
                byte[] rawData = Arrays.copyOf(packet.getData(), packet.getLength());
                byte[] verifiedData = verifyAndExtractData(rawData);
                if (verifiedData == null) {
                    continue; // HMAC verification failed
                }

                ByteBuffer buffer = ByteBuffer.wrap(verifiedData);
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
     * Handle an incoming gossip message with robust error handling.
     */
    private void handleMessage(ByteBuffer buffer, InetAddress senderAddress, int senderPort) {
        if (buffer.remaining() < 1) {
            logger.warn("Empty gossip message from {}:{}", senderAddress, senderPort);
            return;
        }

        byte msgType = buffer.get();
        try {
            if (buffer.remaining() < 8) {
                logger.warn("Malformed gossip message (type={}) from {}:{}: missing timestamp",
                        msgType, senderAddress, senderPort);
                return;
            }
            long timestamp = buffer.getLong();
            String senderId = readString(buffer);
            if (!validateMessage(senderId, timestamp)) {
                return;
            }
            switch (msgType) {
                case MSG_JOIN:
                    handleJoin(buffer, senderId);
                    break;
                case MSG_LEAVE:
                    handleLeave(senderId);
                    break;
                case MSG_HEARTBEAT:
                    handleHeartbeat(senderId, timestamp);
                    break;
                case MSG_MEMBERS:
                    handleMembers(buffer);
                    break;
                case MSG_ACK:
                    // Acknowledgment received
                    break;
                default:
                    logger.warn("Unknown gossip message type: {} from {}:{}",
                            msgType, senderAddress, senderPort);
            }
        } catch (BufferUnderflowException e) {
            logger.warn("Malformed gossip message (type={}) from {}:{}: buffer underflow",
                    msgType, senderAddress, senderPort);
        } catch (IllegalArgumentException e) {
            logger.warn("Invalid gossip message (type={}) from {}:{}: {}",
                    msgType, senderAddress, senderPort, e.getMessage());
        } catch (RuntimeException e) {
            logger.error("Error handling gossip message (type={}) from {}:{}: {}",
                    msgType, senderAddress, senderPort, e.getMessage(), e);
        }
    }

    private void handleJoin(ByteBuffer buffer, String nodeId) {
        String host = readString(buffer);
        int port = buffer.getInt();

        Node node = new Node(nodeId, host, port);
        clusterManager.addNode(node);

        // Send current membership to the joining node
        ByteBuffer members = createMembersList();
        send(members, node);
    }

    private void handleLeave(String nodeId) {
        Node node = clusterManager.getNode(nodeId);
        if (node != null) {
            clusterManager.removeNode(node);
        }
    }

    private void handleHeartbeat(String nodeId, long timestamp) {
        clusterManager.updateHeartbeat(nodeId);
    }

    private void handleMembers(ByteBuffer buffer) {
        if (buffer.remaining() < 4) {
            logger.warn("Malformed members message: insufficient data for count");
            return;
        }

        int count = buffer.getInt();
        if (count < 0 || count > MAX_MEMBERS_COUNT) {
            logger.warn("Invalid member count: {}", count);
            return;
        }

        for (int i = 0; i < count; i++) {
            try {
                String nodeId = readString(buffer);
                String host = readString(buffer);

                if (buffer.remaining() < 5) { // int + byte
                    logger.warn("Malformed members message: insufficient data for member {}", i);
                    break;
                }

                int port = buffer.getInt();
                byte statusOrdinal = buffer.get();

                // Validate status ordinal
                if (statusOrdinal < 0 || statusOrdinal >= Node.Status.values().length) {
                    logger.warn("Invalid status ordinal: {} for node {}", statusOrdinal, nodeId);
                    continue;
                }

                if (!nodeId.equals(localNode.getId())) {
                    Node existing = clusterManager.getNode(nodeId);
                    if (existing == null) {
                        Node node = new Node(nodeId, host, port);
                        node.setStatus(Node.Status.values()[statusOrdinal]);
                        clusterManager.addNode(node);
                    }
                }
            } catch (BufferUnderflowException | IllegalArgumentException e) {
                logger.warn("Error parsing member {}: {}", i, e.getMessage());
                break;
            }
        }
    }

    /**
     * Send a message to a node with optional HMAC authentication.
     */
    private void send(ByteBuffer buffer, Node target) {
        try {
            buffer.flip();
            byte[] data = new byte[buffer.remaining()];
            buffer.get(data);

            // Add HMAC if authentication is enabled
            byte[] packetData;
            if (hmacKey != null) {
                byte[] hmac = computeHmac(data);
                packetData = new byte[data.length + hmac.length];
                System.arraycopy(data, 0, packetData, 0, data.length);
                System.arraycopy(hmac, 0, packetData, data.length, hmac.length);
            } else {
                packetData = data;
            }

            // Calculate target gossip port with overflow protection
            int targetGossipPort = target.getPort() + GOSSIP_PORT_OFFSET;
            if (targetGossipPort > 65535) {
                targetGossipPort = target.getPort() + 100;
            }

            InetAddress address = InetAddress.getByName(target.getHost());
            DatagramPacket packet = new DatagramPacket(packetData, packetData.length, address, targetGossipPort);
            socket.send(packet);
        } catch (IOException e) {
            logger.debug("Failed to send gossip to {}: {}", target.getId(), e.getMessage());
        }
    }

    /**
     * Compute HMAC for message data.
     */
    private byte[] computeHmac(byte[] data) {
        try {
            Mac mac = Mac.getInstance(HMAC_ALGORITHM);
            mac.init(hmacKey);
            return mac.doFinal(data);
        } catch (Exception e) {
            throw new RuntimeException("HMAC computation failed", e);
        }
    }

    /**
     * Verify HMAC of received packet.
     * 
     * @return the data portion without HMAC, or null if verification fails
     */
    private byte[] verifyAndExtractData(byte[] packet) {
        if (hmacKey == null) {
            return packet; // No authentication configured
        }

        if (packet.length < HMAC_SIZE) {
            logger.warn("Packet too small for HMAC verification: {} bytes", packet.length);
            return null;
        }

        byte[] data = Arrays.copyOf(packet, packet.length - HMAC_SIZE);
        byte[] receivedHmac = Arrays.copyOfRange(packet, packet.length - HMAC_SIZE, packet.length);
        byte[] computedHmac = computeHmac(data);

        if (!MessageDigest.isEqual(receivedHmac, computedHmac)) {
            logger.warn("HMAC verification failed - rejecting gossip message");
            return null;
        }

        return data;
    }

    private void writeString(ByteBuffer buffer, String str) {
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        buffer.putShort((short) bytes.length);
        buffer.put(bytes);
    }

    /**
     * Read a string from the buffer with validation.
     * 
     * @throws BufferUnderflowException if buffer doesn't have enough data
     * @throws IllegalArgumentException if string length is invalid
     */
    private String readString(ByteBuffer buffer) {
        if (buffer.remaining() < 2) {
            throw new BufferUnderflowException();
        }
        short length = buffer.getShort();
        if (length < 0 || length > MAX_STRING_LENGTH) {
            throw new IllegalArgumentException("Invalid string length: " + length);
        }
        if (buffer.remaining() < length) {
            throw new BufferUnderflowException();
        }
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private boolean validateMessage(String senderId, long timestamp) {
        long now = System.currentTimeMillis();
        if (timestamp < now - MAX_MESSAGE_AGE_MS || timestamp > now + MAX_MESSAGE_FUTURE_MS) {
            logger.warn("Dropping gossip message from {}: invalid timestamp {}", senderId, timestamp);
            return false;
        }
        if (senderId == null || senderId.isEmpty()) {
            return false;
        }
        Long last = lastMessageTimestamps.get(senderId);
        if (last != null && timestamp <= last) {
            logger.warn("Dropping replayed gossip message from {} (ts={})", senderId, timestamp);
            return false;
        }
        lastMessageTimestamps.put(senderId, timestamp);
        return true;
    }

    /**
     * Get the gossip port.
     */
    public int getGossipPort() {
        return gossipPort;
    }
}
