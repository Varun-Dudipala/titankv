package com.titankv.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.net.*;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * UDP gossip for cluster membership and failure detection, in the style of Cassandra.
 *
 * Every node owns a heartbeat (generation, version): the generation is its start time and the
 * version increases once per gossip round. Each round a node bumps its own version and sends a
 * digest of every member's heartbeat to a few random peers. A receiver adopts any heartbeat newer
 * than the one it knows and treats that as having heard from the node, so liveness spreads
 * transitively and each node does not need to hear from every other node directly. Unknown
 * members in a digest are added, so new nodes become known cluster-wide.
 *
 * Wire format (big endian), optionally followed by a 32-byte HMAC-SHA256 of the message:
 *   header:  [type:1][sentAt:8][senderId:str]
 *   JOIN:    header [host:str][port:4][generation:8][version:8]
 *   LEAVE:   header
 *   DIGEST:  header [count:4] then per member [id:str][host:str][port:4][status:1][generation:8][version:8]
 * where str is [length:2][UTF-8 bytes]. sentAt strictly increases per sender, which lets the
 * receiver reject replayed packets.
 */
public class GossipProtocol {

    private static final Logger logger = LoggerFactory.getLogger(GossipProtocol.class);

    private static final byte MSG_JOIN = 0x01;
    private static final byte MSG_LEAVE = 0x02;
    private static final byte MSG_DIGEST = 0x04;

    private static final int GOSSIP_PORT_OFFSET = 1000;
    private static final int GOSSIP_INTERVAL_MS = 1000;
    private static final int GOSSIP_FANOUT = 3;
    private static final double UNREACHABLE_GOSSIP_PROBABILITY = 0.25;
    private static final int MAX_PACKET_SIZE = 1400; // stays under a 1500-byte MTU
    private static final int HMAC_SIZE = 32;
    private static final int MAX_STRING_LENGTH = 1024;
    private static final int MAX_MEMBERS_COUNT = 1000;
    private static final String HMAC_ALGORITHM = "HmacSHA256";
    private static final long MAX_MESSAGE_AGE_MS = 5 * 60 * 1000;
    private static final long MAX_MESSAGE_FUTURE_MS = 60 * 1000;

    private final Node localNode;
    private final ClusterManager clusterManager;
    private final int gossipPort;
    private final SecretKeySpec hmacKey;
    private final long generation = System.currentTimeMillis();
    private final AtomicLong heartbeatVersion = new AtomicLong();
    private final AtomicLong lastSentAt = new AtomicLong();
    private final ConcurrentHashMap<String, Long> lastReceivedAt = new ConcurrentHashMap<>();
    private volatile List<Node> seeds = List.of();

    private DatagramSocket socket;
    private ExecutorService receiver;
    private ScheduledExecutorService scheduler;
    private volatile boolean running;

    public GossipProtocol(Node localNode, ClusterManager clusterManager) {
        this(localNode, clusterManager, null);
    }

    /**
     * @param clusterSecret shared secret for HMAC-signing every packet, or null to disable (dev mode)
     */
    public GossipProtocol(Node localNode, ClusterManager clusterManager, String clusterSecret) {
        this.localNode = localNode;
        this.clusterManager = clusterManager;
        this.gossipPort = gossipPortFor(localNode.getPort());
        this.hmacKey = clusterSecret != null && !clusterSecret.isEmpty()
                ? new SecretKeySpec(clusterSecret.getBytes(StandardCharsets.UTF_8), HMAC_ALGORITHM)
                : null;
        localNode.advanceHeartbeat(generation, 0);
    }

    private static int gossipPortFor(int port) {
        int gossip = port + GOSSIP_PORT_OFFSET;
        if (gossip > 65535) {
            gossip = port + 100;
            if (gossip > 65535) {
                throw new IllegalArgumentException("Cannot compute valid gossip port for base port: " + port);
            }
        }
        return gossip;
    }

    /**
     * Nodes to send JOIN to until another member is known.
     */
    public void setSeeds(List<Node> seeds) {
        this.seeds = List.copyOf(seeds);
    }

    public void start() {
        if (running) {
            return;
        }
        try {
            socket = new DatagramSocket(gossipPort);
            socket.setSoTimeout(500);
        } catch (SocketException e) {
            logger.error("Failed to start gossip on port {}: {}", gossipPort, e.getMessage());
            return;
        }
        running = true;

        receiver = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "gossip-receiver");
            t.setDaemon(true);
            return t;
        });
        receiver.submit(this::receiveLoop);

        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "gossip-sender");
            t.setDaemon(true);
            return t;
        });
        joinSeeds();
        scheduler.scheduleAtFixedRate(this::gossipRound, GOSSIP_INTERVAL_MS, GOSSIP_INTERVAL_MS,
                TimeUnit.MILLISECONDS);

        logger.info("Gossip protocol started on port {}", gossipPort);
    }

    public void stop() {
        if (!running) {
            return;
        }
        broadcastLeave();
        running = false;

        shutdown(scheduler);
        shutdown(receiver);
        if (socket != null) {
            socket.close();
        }
        logger.info("Gossip protocol stopped");
    }

    private static void shutdown(ExecutorService executor) {
        if (executor == null) {
            return;
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public int getGossipPort() {
        return gossipPort;
    }

    // ==================== Sending ====================

    private void gossipRound() {
        if (!running) {
            return;
        }
        try {
            localNode.advanceHeartbeat(generation, heartbeatVersion.incrementAndGet());
            if (clusterManager.getNodeCount() <= 1) {
                joinSeeds();
                return;
            }

            List<Node> reachable = new ArrayList<>();
            List<Node> unreachable = new ArrayList<>();
            for (Node node : clusterManager.getAllNodes()) {
                if (!node.equals(localNode)) {
                    (node.isAvailable() ? reachable : unreachable).add(node);
                }
            }
            ThreadLocalRandom random = ThreadLocalRandom.current();
            Collections.shuffle(reachable, random);
            List<Node> targets = new ArrayList<>(reachable.subList(0, Math.min(GOSSIP_FANOUT, reachable.size())));
            // Occasionally contact a node we consider down so a recovered node or healed
            // partition is noticed even if it never contacts us first.
            if (!unreachable.isEmpty() && random.nextDouble() < UNREACHABLE_GOSSIP_PROBABILITY) {
                targets.add(unreachable.get(random.nextInt(unreachable.size())));
            }
            for (Node target : targets) {
                send(createDigest(), target);
            }
        } catch (RuntimeException e) {
            // An exception would cancel the scheduled task and silently stop gossip for good
            logger.error("Gossip round failed", e);
        }
    }

    private void joinSeeds() {
        for (Node seed : seeds) {
            ByteBuffer buffer = newMessage(MSG_JOIN);
            writeString(buffer, localNode.getHost());
            buffer.putInt(localNode.getPort());
            buffer.putLong(generation);
            buffer.putLong(heartbeatVersion.get());
            send(buffer, seed);
        }
    }

    private void broadcastLeave() {
        for (Node node : clusterManager.getAllNodes()) {
            if (!node.equals(localNode)) {
                send(newMessage(MSG_LEAVE), node);
            }
        }
    }

    /**
     * Digest of member heartbeats. The local node always comes first; others are shuffled so
     * that when a large cluster does not fit in one packet, each round carries a different subset.
     */
    private ByteBuffer createDigest() {
        ByteBuffer buffer = newMessage(MSG_DIGEST);
        int countPosition = buffer.position();
        buffer.putInt(0);

        List<Node> others = new ArrayList<>(clusterManager.getAllNodes());
        others.remove(localNode);
        Collections.shuffle(others, ThreadLocalRandom.current());
        List<Node> members = new ArrayList<>(others.size() + 1);
        members.add(localNode);
        members.addAll(others);

        int count = 0;
        for (Node node : members) {
            int mark = buffer.position();
            try {
                writeString(buffer, node.getId());
                writeString(buffer, node.getHost());
                buffer.putInt(node.getPort());
                buffer.put((byte) node.getStatus().ordinal());
                buffer.putLong(node.getGeneration());
                buffer.putLong(node.getHeartbeatVersion());
                count++;
            } catch (BufferOverflowException e) {
                buffer.position(mark);
                break;
            }
            if (count >= MAX_MEMBERS_COUNT) {
                break;
            }
        }
        buffer.putInt(countPosition, count);
        return buffer;
    }

    private ByteBuffer newMessage(byte type) {
        ByteBuffer buffer = ByteBuffer.allocate(MAX_PACKET_SIZE - HMAC_SIZE);
        buffer.put(type);
        buffer.putLong(nextSentAt());
        writeString(buffer, localNode.getId());
        return buffer;
    }

    /**
     * Wall-clock time, bumped by 1ms when needed so it strictly increases for every message.
     */
    private long nextSentAt() {
        long now = System.currentTimeMillis();
        return lastSentAt.updateAndGet(last -> Math.max(now, last + 1));
    }

    private void send(ByteBuffer message, Node target) {
        DatagramSocket out = socket;
        if (out == null) {
            return;
        }
        try {
            message.flip();
            byte[] data = new byte[message.remaining()];
            message.get(data);
            byte[] packet = data;
            if (hmacKey != null) {
                byte[] hmac = computeHmac(data);
                packet = Arrays.copyOf(data, data.length + hmac.length);
                System.arraycopy(hmac, 0, packet, data.length, hmac.length);
            }
            InetAddress address = InetAddress.getByName(target.getHost());
            out.send(new DatagramPacket(packet, packet.length, address, gossipPortFor(target.getPort())));
        } catch (IOException e) {
            logger.debug("Failed to send gossip to {}: {}", target.getId(), e.getMessage());
        }
    }

    // ==================== Receiving ====================

    private void receiveLoop() {
        byte[] receiveBuffer = new byte[MAX_PACKET_SIZE];
        DatagramPacket packet = new DatagramPacket(receiveBuffer, receiveBuffer.length);
        while (running) {
            try {
                socket.receive(packet);
                byte[] data = verifyAndExtractData(Arrays.copyOf(packet.getData(), packet.getLength()));
                if (data != null) {
                    handleMessage(ByteBuffer.wrap(data), packet.getAddress(), packet.getPort());
                }
            } catch (SocketTimeoutException e) {
                // lets the loop observe running=false
            } catch (IOException e) {
                if (running) {
                    logger.error("Error receiving gossip: {}", e.getMessage());
                }
            }
        }
    }

    private void handleMessage(ByteBuffer buffer, InetAddress from, int fromPort) {
        byte type = 0;
        try {
            type = buffer.get();
            long sentAt = buffer.getLong();
            String senderId = readString(buffer);
            if (senderId.isEmpty() || senderId.equals(localNode.getId()) || !isFresh(senderId, sentAt)) {
                return;
            }
            switch (type) {
                case MSG_JOIN:
                    handleJoin(buffer, senderId);
                    break;
                case MSG_LEAVE:
                    Node leaving = clusterManager.getNode(senderId);
                    if (leaving != null) {
                        clusterManager.removeNode(leaving);
                    }
                    break;
                case MSG_DIGEST:
                    handleDigest(buffer);
                    break;
                default:
                    logger.warn("Unknown gossip message type {} from {}:{}", type, from, fromPort);
            }
        } catch (BufferUnderflowException | IllegalArgumentException e) {
            logger.warn("Malformed gossip message (type={}) from {}:{}: {}", type, from, fromPort, e.toString());
        } catch (RuntimeException e) {
            logger.error("Error handling gossip message (type={}) from {}:{}", type, from, fromPort, e);
        }
    }

    private void handleJoin(ByteBuffer buffer, String nodeId) {
        String host = readString(buffer);
        int port = buffer.getInt();
        long joinGeneration = buffer.getLong();
        long joinVersion = buffer.getLong();
        learn(nodeId, host, port, Node.Status.ALIVE, joinGeneration, joinVersion);
        Node joined = clusterManager.getNode(nodeId);
        if (joined != null) {
            send(createDigest(), joined);
        }
    }

    private void handleDigest(ByteBuffer buffer) {
        int count = buffer.getInt();
        if (count < 0 || count > MAX_MEMBERS_COUNT) {
            throw new IllegalArgumentException("Invalid member count: " + count);
        }
        Node.Status[] statuses = Node.Status.values();
        for (int i = 0; i < count; i++) {
            String id = readString(buffer);
            String host = readString(buffer);
            int port = buffer.getInt();
            int statusOrdinal = buffer.get();
            long memberGeneration = buffer.getLong();
            long memberVersion = buffer.getLong();
            if (statusOrdinal < 0 || statusOrdinal >= statuses.length) {
                throw new IllegalArgumentException("Invalid status ordinal: " + statusOrdinal);
            }
            if (!id.equals(localNode.getId())) {
                learn(id, host, port, statuses[statusOrdinal], memberGeneration, memberVersion);
            }
        }
    }

    /**
     * Apply one member's gossiped heartbeat. A newer heartbeat counts as hearing from the node.
     */
    private void learn(String id, String host, int port, Node.Status reportedStatus,
            long memberGeneration, long memberVersion) {
        Node known = clusterManager.getNode(id);
        if (known != null) {
            long previousGeneration = known.getGeneration();
            if (known.advanceHeartbeat(memberGeneration, memberVersion)) {
                clusterManager.updateHeartbeat(id);
                if (previousGeneration != 0 && memberGeneration > previousGeneration) {
                    clusterManager.nodeRestarted(known);
                }
            }
            return;
        }
        // Only adopt members someone currently believes are up, and never resurrect a node
        // that has gracefully left unless it has restarted since (newer generation).
        boolean reportedUp = reportedStatus == Node.Status.ALIVE || reportedStatus == Node.Status.JOINING;
        if (!reportedUp || clusterManager.hasDeparted(id, memberGeneration)) {
            return;
        }
        Node node = new Node(id, host, port);
        node.advanceHeartbeat(memberGeneration, memberVersion);
        node.setStatus(Node.Status.ALIVE);
        clusterManager.addNode(node);
    }

    // ==================== Validation ====================

    private boolean isFresh(String senderId, long sentAt) {
        long now = System.currentTimeMillis();
        if (sentAt < now - MAX_MESSAGE_AGE_MS || sentAt > now + MAX_MESSAGE_FUTURE_MS) {
            logger.warn("Dropping gossip message from {}: timestamp {} outside the accepted window", senderId, sentAt);
            return false;
        }
        boolean[] fresh = {false};
        lastReceivedAt.compute(senderId, (id, last) -> {
            if (last != null && sentAt <= last) {
                return last;
            }
            fresh[0] = true;
            return sentAt;
        });
        if (!fresh[0]) {
            // Expected occasionally when UDP reorders packets; only a problem if frequent
            logger.debug("Dropping replayed or reordered gossip message from {} (sentAt={})", senderId, sentAt);
        }
        return fresh[0];
    }

    private byte[] computeHmac(byte[] data) {
        try {
            Mac mac = Mac.getInstance(HMAC_ALGORITHM);
            mac.init(hmacKey);
            return mac.doFinal(data);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("HMAC computation failed", e);
        }
    }

    /**
     * @return the message without its HMAC, or null if authentication is enabled and the HMAC is wrong
     */
    private byte[] verifyAndExtractData(byte[] packet) {
        if (hmacKey == null) {
            return packet;
        }
        if (packet.length < HMAC_SIZE) {
            logger.warn("Packet too small for HMAC verification: {} bytes", packet.length);
            return null;
        }
        byte[] data = Arrays.copyOf(packet, packet.length - HMAC_SIZE);
        byte[] received = Arrays.copyOfRange(packet, packet.length - HMAC_SIZE, packet.length);
        if (!MessageDigest.isEqual(received, computeHmac(data))) {
            logger.warn("HMAC verification failed - rejecting gossip message");
            return null;
        }
        return data;
    }

    private static void writeString(ByteBuffer buffer, String str) {
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > MAX_STRING_LENGTH) {
            throw new IllegalArgumentException("String too long for gossip: " + bytes.length + " bytes");
        }
        buffer.putShort((short) bytes.length);
        buffer.put(bytes);
    }

    private static String readString(ByteBuffer buffer) {
        int length = buffer.getShort();
        if (length < 0 || length > MAX_STRING_LENGTH) {
            throw new IllegalArgumentException("Invalid string length: " + length);
        }
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
