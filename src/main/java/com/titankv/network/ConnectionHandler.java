package com.titankv.network;

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
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Optional;

/**
 * Handles individual client connections.
 * Manages read/write buffers and request processing.
 */
public class ConnectionHandler {

    private static final Logger logger = LoggerFactory.getLogger(ConnectionHandler.class);
    private static final int BUFFER_SIZE = 64 * 1024;

    private final SocketChannel channel;
    private final KVStore store;
    private final MetricsCollector metrics;
    private final ByteBuffer readBuffer;
    private final ByteBuffer writeBuffer;
    private final String clientAddress;

    private boolean writeInProgress = false;

    public ConnectionHandler(SocketChannel channel, KVStore store, MetricsCollector metrics) {
        this.channel = channel;
        this.store = store;
        this.metrics = metrics;
        this.readBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
        this.writeBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
        this.clientAddress = getClientAddress();
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
     * Handle a read event from the selector.
     *
     * @param key the selection key
     * @return true if the connection should continue, false to close
     */
    public boolean handleRead(SelectionKey key) {
        try {
            int bytesRead = channel.read(readBuffer);
            if (bytesRead == -1) {
                logger.debug("Client {} disconnected", clientAddress);
                return false;
            }

            if (bytesRead > 0) {
                processReadBuffer(key);
            }
            return true;
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
                Response response = processCommand(command);
                queueResponse(response, key);
            } catch (ProtocolException e) {
                logger.warn("Protocol error from {}: {}", clientAddress, e.getMessage());
                queueResponse(Response.error(e.getMessage()), key);
            }
        }

        readBuffer.compact();
    }

    private Response processCommand(Command command) {
        long startTime = System.nanoTime();
        Response response;

        try {
            switch (command.getType()) {
                case Command.GET:
                    response = handleGet(command);
                    metrics.recordGet(System.nanoTime() - startTime,
                        response.getStatus() == Response.OK);
                    break;

                case Command.PUT:
                    response = handlePut(command);
                    metrics.recordPut(System.nanoTime() - startTime);
                    break;

                case Command.DELETE:
                    response = handleDelete(command);
                    metrics.recordDelete(System.nanoTime() - startTime);
                    break;

                case Command.PING:
                    response = Response.pong();
                    break;

                case Command.EXISTS:
                    response = handleExists(command);
                    break;

                default:
                    logger.warn("Unknown command type: {}", command.getType());
                    response = Response.error("Unknown command");
                    metrics.recordError();
            }
        } catch (Exception e) {
            logger.error("Error processing command {}: {}", command.getTypeName(), e.getMessage());
            response = Response.error("Internal error: " + e.getMessage());
            metrics.recordError();
        }

        logger.trace("{} {} -> {}", command.getTypeName(), command.getKey(),
            response.getStatusName());
        return response;
    }

    private Response handleGet(Command command) {
        if (command.getKey() == null) {
            return Response.error("Key required for GET");
        }
        Optional<KeyValuePair> result = store.get(command.getKey());
        if (result.isPresent()) {
            return Response.ok(result.get().getValue());
        }
        return Response.notFound();
    }

    private Response handlePut(Command command) {
        if (command.getKey() == null) {
            return Response.error("Key required for PUT");
        }
        store.put(command.getKey(), command.getValueUnsafe());
        return Response.ok();
    }

    private Response handleDelete(Command command) {
        if (command.getKey() == null) {
            return Response.error("Key required for DELETE");
        }
        store.delete(command.getKey());
        return Response.ok();
    }

    private Response handleExists(Command command) {
        if (command.getKey() == null) {
            return Response.error("Key required for EXISTS");
        }
        return Response.exists(store.exists(command.getKey()));
    }

    private void queueResponse(Response response, SelectionKey key) {
        ByteBuffer encoded = BinaryProtocol.encode(response);

        if (writeBuffer.remaining() >= encoded.remaining()) {
            writeBuffer.put(encoded);

            if (!writeInProgress) {
                writeInProgress = true;
                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            }
        } else {
            logger.warn("Write buffer full, dropping response to {}", clientAddress);
        }
    }

    /**
     * Handle a write event from the selector.
     *
     * @param key the selection key
     * @return true if the connection should continue, false to close
     */
    public boolean handleWrite(SelectionKey key) {
        try {
            writeBuffer.flip();
            channel.write(writeBuffer);
            writeBuffer.compact();

            if (writeBuffer.position() == 0) {
                writeInProgress = false;
                key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
            }
            return true;
        } catch (IOException e) {
            logger.warn("Write error to {}: {}", clientAddress, e.getMessage());
            return false;
        }
    }

    /**
     * Close this connection and release resources.
     */
    public void close() {
        try {
            channel.close();
        } catch (IOException e) {
            logger.debug("Error closing connection: {}", e.getMessage());
        }
        metrics.connectionClosed();
        logger.debug("Connection closed: {}", clientAddress);
    }

    public String getRemoteAddress() {
        return clientAddress;
    }
}
