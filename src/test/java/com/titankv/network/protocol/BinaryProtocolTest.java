package com.titankv.network.protocol;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.*;

class BinaryProtocolTest {

    @Test
    void encodeDecodeGet_roundTrip() {
        Command original = Command.get("testKey");

        ByteBuffer buffer = BinaryProtocol.encode(original);
        Command decoded = BinaryProtocol.decodeCommand(buffer);

        assertThat(decoded.getType()).isEqualTo(Command.GET);
        assertThat(decoded.getKey()).isEqualTo("testKey");
        assertThat(decoded.getValue()).isNull();
    }

    @Test
    void encodeDecodePut_roundTrip() {
        byte[] value = "testValue".getBytes(StandardCharsets.UTF_8);
        Command original = Command.put("testKey", value);

        ByteBuffer buffer = BinaryProtocol.encode(original);
        Command decoded = BinaryProtocol.decodeCommand(buffer);

        assertThat(decoded.getType()).isEqualTo(Command.PUT);
        assertThat(decoded.getKey()).isEqualTo("testKey");
        assertThat(decoded.getValue()).isEqualTo(value);
    }

    @Test
    void encodeDecodeDelete_roundTrip() {
        Command original = Command.delete("keyToDelete");

        ByteBuffer buffer = BinaryProtocol.encode(original);
        Command decoded = BinaryProtocol.decodeCommand(buffer);

        assertThat(decoded.getType()).isEqualTo(Command.DELETE);
        assertThat(decoded.getKey()).isEqualTo("keyToDelete");
        assertThat(decoded.getValue()).isNull();
    }

    @Test
    void encodeDecodePing_roundTrip() {
        Command original = Command.ping();

        ByteBuffer buffer = BinaryProtocol.encode(original);
        Command decoded = BinaryProtocol.decodeCommand(buffer);

        assertThat(decoded.getType()).isEqualTo(Command.PING);
        assertThat(decoded.getKey()).isNull();
        assertThat(decoded.getValue()).isNull();
    }

    @Test
    void encodeDecodeResponse_ok() {
        byte[] value = "resultValue".getBytes(StandardCharsets.UTF_8);
        Response original = Response.ok(value);

        ByteBuffer buffer = BinaryProtocol.encode(original);
        Response decoded = BinaryProtocol.decodeResponse(buffer);

        assertThat(decoded.getStatus()).isEqualTo(Response.OK);
        assertThat(decoded.getValue()).isEqualTo(value);
        assertThat(decoded.isOk()).isTrue();
    }

    @Test
    void encodeDecodeResponse_notFound() {
        Response original = Response.notFound();

        ByteBuffer buffer = BinaryProtocol.encode(original);
        Response decoded = BinaryProtocol.decodeResponse(buffer);

        assertThat(decoded.getStatus()).isEqualTo(Response.NOT_FOUND);
        assertThat(decoded.isNotFound()).isTrue();
        assertThat(decoded.hasValue()).isFalse();
    }

    @Test
    void encodeDecodeResponse_error() {
        Response original = Response.error("Something went wrong");

        ByteBuffer buffer = BinaryProtocol.encode(original);
        Response decoded = BinaryProtocol.decodeResponse(buffer);

        assertThat(decoded.getStatus()).isEqualTo(Response.ERROR);
        assertThat(decoded.isError()).isTrue();
        assertThat(decoded.getErrorMessage()).isEqualTo("Something went wrong");
    }

    @Test
    void encodeDecodeResponse_pong() {
        Response original = Response.pong();

        ByteBuffer buffer = BinaryProtocol.encode(original);
        Response decoded = BinaryProtocol.decodeResponse(buffer);

        assertThat(decoded.getStatus()).isEqualTo(Response.PONG);
        assertThat(decoded.isOk()).isTrue();
    }

    @Test
    void decode_invalidMagic_throwsException() {
        ByteBuffer buffer = ByteBuffer.allocate(40);
        buffer.putInt(0xDEADBEEF); // Wrong magic
        buffer.put(Command.GET);
        buffer.putInt(4);           // Key length
        buffer.putInt(0);           // Value length
        buffer.putLong(0);          // Timestamp
        buffer.putLong(0);          // ExpiresAt
        buffer.put("test".getBytes(StandardCharsets.UTF_8));
        buffer.flip();

        assertThatThrownBy(() -> BinaryProtocol.decodeCommand(buffer))
            .isInstanceOf(ProtocolException.class)
            .hasMessageContaining("Invalid magic bytes");
    }

    @Test
    void decode_incompleteHeader_throwsException() {
        ByteBuffer buffer = ByteBuffer.allocate(5);
        buffer.putInt(BinaryProtocol.MAGIC);
        buffer.put(Command.GET);
        buffer.flip();

        assertThatThrownBy(() -> BinaryProtocol.decodeCommand(buffer))
            .isInstanceOf(ProtocolException.class)
            .hasMessageContaining("Incomplete header");
    }

    @Test
    void decode_incompletePayload_throwsException() {
        ByteBuffer buffer = ByteBuffer.allocate(BinaryProtocol.REQUEST_HEADER_SIZE);
        buffer.putInt(BinaryProtocol.MAGIC);
        buffer.put(Command.GET);
        buffer.putInt(100); // Key length of 100
        buffer.putInt(0);   // Value length
        buffer.putLong(0);  // Timestamp
        buffer.putLong(0);  // ExpiresAt
        buffer.flip();

        assertThatThrownBy(() -> BinaryProtocol.decodeCommand(buffer))
            .isInstanceOf(ProtocolException.class)
            .hasMessageContaining("Incomplete payload");
    }

    @Test
    void decode_negativeKeyLength_throwsException() {
        ByteBuffer buffer = ByteBuffer.allocate(BinaryProtocol.REQUEST_HEADER_SIZE);
        buffer.putInt(BinaryProtocol.MAGIC);
        buffer.put(Command.GET);
        buffer.putInt(-1);  // Invalid negative length
        buffer.putInt(0);   // Value length
        buffer.putLong(0);  // Timestamp
        buffer.putLong(0);  // ExpiresAt
        buffer.flip();

        assertThatThrownBy(() -> BinaryProtocol.decodeCommand(buffer))
            .isInstanceOf(ProtocolException.class)
            .hasMessageContaining("Invalid key length");
    }

    @Test
    void hasCompleteRequest_complete_returnsTrue() {
        Command cmd = Command.put("key", "value".getBytes(StandardCharsets.UTF_8));
        ByteBuffer buffer = BinaryProtocol.encode(cmd);

        assertThat(BinaryProtocol.hasCompleteRequest(buffer)).isTrue();
    }

    @Test
    void hasCompleteRequest_incomplete_returnsFalse() {
        ByteBuffer buffer = ByteBuffer.allocate(5);
        buffer.putInt(BinaryProtocol.MAGIC);
        buffer.put(Command.GET);
        buffer.flip();

        assertThat(BinaryProtocol.hasCompleteRequest(buffer)).isFalse();
    }

    @Test
    void hasCompleteResponse_complete_returnsTrue() {
        Response resp = Response.ok("value".getBytes(StandardCharsets.UTF_8));
        ByteBuffer buffer = BinaryProtocol.encode(resp);

        assertThat(BinaryProtocol.hasCompleteResponse(buffer)).isTrue();
    }

    @Test
    void hasCompleteResponse_incomplete_returnsFalse() {
        ByteBuffer buffer = ByteBuffer.allocate(5);
        buffer.putInt(BinaryProtocol.MAGIC);
        buffer.put(Response.OK);
        buffer.flip();

        assertThat(BinaryProtocol.hasCompleteResponse(buffer)).isFalse();
    }

    @Test
    void encodedSize_command_calculatesCorrectly() {
        Command cmd = Command.put("key", "value".getBytes(StandardCharsets.UTF_8));
        int expectedSize = BinaryProtocol.REQUEST_HEADER_SIZE + 3 + 5; // 29 + key(3) + value(5)

        assertThat(BinaryProtocol.encodedSize(cmd)).isEqualTo(expectedSize);
    }

    @Test
    void encodedSize_response_calculatesCorrectly() {
        Response resp = Response.ok("value".getBytes(StandardCharsets.UTF_8));
        int expectedSize = BinaryProtocol.RESPONSE_HEADER_SIZE + 5; // 9 + value(5)

        assertThat(BinaryProtocol.encodedSize(resp)).isEqualTo(expectedSize);
    }

    @Test
    void encodeIntoBuffer_command() {
        Command cmd = Command.get("testKey");
        ByteBuffer buffer = ByteBuffer.allocate(100);

        BinaryProtocol.encode(cmd, buffer);
        buffer.flip();

        Command decoded = BinaryProtocol.decodeCommand(buffer);
        assertThat(decoded.getKey()).isEqualTo("testKey");
    }

    @Test
    void encodeIntoBuffer_response() {
        byte[] value = "result".getBytes(StandardCharsets.UTF_8);
        Response resp = Response.ok(value);
        ByteBuffer buffer = ByteBuffer.allocate(100);

        BinaryProtocol.encode(resp, buffer);
        buffer.flip();

        Response decoded = BinaryProtocol.decodeResponse(buffer);
        assertThat(decoded.getValue()).isEqualTo(value);
    }

    @Test
    void largeValue_roundTrip() {
        byte[] largeValue = new byte[100_000];
        for (int i = 0; i < largeValue.length; i++) {
            largeValue[i] = (byte) (i % 256);
        }

        Command original = Command.put("largeKey", largeValue);
        ByteBuffer buffer = BinaryProtocol.encode(original);
        Command decoded = BinaryProtocol.decodeCommand(buffer);

        assertThat(decoded.getValue()).isEqualTo(largeValue);
    }

    @Test
    void unicodeKey_roundTrip() {
        String unicodeKey = "key-日本語-emoji-\uD83D\uDE00";
        Command original = Command.get(unicodeKey);

        ByteBuffer buffer = BinaryProtocol.encode(original);
        Command decoded = BinaryProtocol.decodeCommand(buffer);

        assertThat(decoded.getKey()).isEqualTo(unicodeKey);
    }

    @Test
    void emptyValue_roundTrip() {
        Command original = Command.put("key", new byte[0]);

        ByteBuffer buffer = BinaryProtocol.encode(original);
        Command decoded = BinaryProtocol.decodeCommand(buffer);

        // Empty byte array should be preserved (distinct from null)
        assertThat(decoded.hasValue()).isTrue();
        assertThat(decoded.getValue()).isNotNull();
        assertThat(decoded.getValue()).isEmpty();
    }

    @Test
    void nullValue_roundTrip() {
        // GET command has null value
        Command original = Command.get("key");

        ByteBuffer buffer = BinaryProtocol.encode(original);
        Command decoded = BinaryProtocol.decodeCommand(buffer);

        // Null value should be preserved (distinct from empty array)
        assertThat(decoded.hasValue()).isFalse();
        assertThat(decoded.getValue()).isNull();
    }
}
