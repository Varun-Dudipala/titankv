package com.titankv.network;

import com.titankv.TitanKVClient;
import com.titankv.TitanKVServer;
import com.titankv.network.protocol.BinaryProtocol;
import com.titankv.network.protocol.Command;
import com.titankv.network.protocol.Response;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Connections that keep the server waiting for a whole request are closed by the server, and the
 * number of connections is limited. Before the selector swept for them, the timeout was only
 * checked when more bytes arrived, so a client that sent part of a request and went quiet kept
 * its connection and buffers for good.
 */
class StalledConnectionTest {

    private static final int PORT = 19030;

    private TitanKVServer server;
    private final List<Socket> sockets = new ArrayList<>();

    @BeforeEach
    void startServer() throws Exception {
        System.setProperty("titankv.frame.timeout.ms", "500");
        System.setProperty("titankv.max.connections", "4");
        server = new TitanKVServer(PORT);
        server.start();
    }

    @AfterEach
    void stopServer() throws IOException {
        for (Socket socket : sockets) {
            socket.close();
        }
        server.stop();
        System.clearProperty("titankv.frame.timeout.ms");
        System.clearProperty("titankv.max.connections");
    }

    private Socket connect() throws IOException {
        Socket socket = new Socket("localhost", PORT);
        socket.setSoTimeout(5_000);
        sockets.add(socket);
        return socket;
    }

    /**
     * @return true if the server closed the connection before the read timed out
     */
    private static boolean closedByServer(Socket socket) throws IOException {
        InputStream in = socket.getInputStream();
        try {
            return in.read() == -1;
        } catch (SocketException e) {
            return true; // reset
        }
    }

    @Test
    void partOfARequestFollowedBySilenceIsClosed() throws Exception {
        Socket socket = connect();
        ByteBuffer request = BinaryProtocol.encode(Command.put("key", new byte[100]));
        socket.getOutputStream().write(request.array(), 0, 9); // the magic and a little more
        socket.getOutputStream().flush();

        assertThat(closedByServer(socket)).isTrue();
        assertThat(server.getConnectionCount()).isZero();
    }

    @Test
    void connectionThatNeverSendsARequestIsClosed() throws Exception {
        Socket socket = connect();

        assertThat(closedByServer(socket)).isTrue();
        assertThat(server.getConnectionCount()).isZero();
    }

    @Test
    void requestAfterACompleteOneThatStopsHalfwayIsClosed() throws Exception {
        Socket socket = connect();
        ByteBuffer ping = BinaryProtocol.encode(Command.ping());
        ByteBuffer put = BinaryProtocol.encode(Command.put("key", new byte[100]));
        byte[] both = new byte[ping.remaining() + 20];
        ping.get(both, 0, ping.remaining());
        put.get(both, both.length - 20, 20);
        socket.getOutputStream().write(both); // a whole PING, then the first 20 bytes of a PUT
        socket.getOutputStream().flush();

        assertThat(readResponse(socket).getStatus()).isEqualTo(Response.PONG);
        assertThat(closedByServer(socket)).isTrue();
    }

    @Test
    void requestThatArrivesInPiecesInTimeIsServed() throws Exception {
        Socket socket = connect();
        ByteBuffer request = BinaryProtocol.encode(Command.put("slow-key", "value".getBytes()));
        byte[] bytes = request.array();
        socket.getOutputStream().write(bytes, 0, 10);
        socket.getOutputStream().flush();
        Thread.sleep(200);
        socket.getOutputStream().write(bytes, 10, bytes.length - 10);
        socket.getOutputStream().flush();

        assertThat(readResponse(socket).isOk()).isTrue();
    }

    @Test
    void connectionIdleBetweenRequestsIsKept() throws Exception {
        try (TitanKVClient client = new TitanKVClient("localhost:" + PORT)) {
            client.put("pooled", "value");
            Thread.sleep(2_500); // five times the timeout, with the pooled connection idle

            assertThat(server.getConnectionCount()).isEqualTo(1);
            assertThat(client.getString("pooled")).contains("value");
            assertThat(client.getConnectionCount()).as("the same connection was reused").isEqualTo(1);
        }
    }

    @Test
    void connectionsBeyondTheLimitAreRefused() throws Exception {
        ByteBuffer ping = BinaryProtocol.encode(Command.ping());
        for (int i = 0; i < 4; i++) {
            Socket socket = connect();
            socket.getOutputStream().write(ping.array());
            socket.getOutputStream().flush();
            assertThat(readResponse(socket).getStatus()).isEqualTo(Response.PONG);
        }
        assertThat(server.getConnectionCount()).isEqualTo(4);

        Socket oneTooMany = connect();
        assertThat(closedByServer(oneTooMany)).isTrue();
        assertThat(server.getConnectionCount()).isEqualTo(4);

        // Closing one makes room again
        sockets.get(0).close();
        long deadline = System.currentTimeMillis() + 5_000;
        while (server.getConnectionCount() > 3 && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        Socket replacement = connect();
        replacement.getOutputStream().write(ping.array());
        replacement.getOutputStream().flush();
        assertThat(readResponse(replacement).getStatus()).isEqualTo(Response.PONG);
    }

    private static Response readResponse(Socket socket) throws IOException {
        DataInputStream in = new DataInputStream(socket.getInputStream());
        byte[] header = new byte[BinaryProtocol.RESPONSE_HEADER_SIZE];
        in.readFully(header);
        int valueLength = Math.max(0, ByteBuffer.wrap(header).getInt(5));
        byte[] frame = new byte[header.length + valueLength];
        System.arraycopy(header, 0, frame, 0, header.length);
        in.readFully(frame, header.length, valueLength);
        return BinaryProtocol.decodeResponse(ByteBuffer.wrap(frame));
    }
}
