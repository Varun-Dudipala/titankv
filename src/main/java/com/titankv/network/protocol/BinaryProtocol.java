package com.titankv.network.protocol;

import com.titankv.util.ByteBufferPool;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Binary wire protocol encoder/decoder for TitanKV.
 *
 * Protocol format:
 * Request:  [MAGIC:4][CMD:1][KEY_LEN:4][VAL_LEN:4][KEY:N][VALUE:M]
 * Response: [MAGIC:4][STATUS:1][VAL_LEN:4][VALUE:N]
 *
 * Header size: 13 bytes (request), 9 bytes (response)
 */
public final class BinaryProtocol {

    // Magic bytes: "TITK" in hex
    public static final int MAGIC = 0x5449544B;

    // Header sizes
    public static final int REQUEST_HEADER_SIZE = 13;  // magic(4) + cmd(1) + keyLen(4) + valLen(4)
    public static final int RESPONSE_HEADER_SIZE = 9;  // magic(4) + status(1) + valLen(4)

    // Maximum sizes
    public static final int MAX_KEY_LENGTH = 64 * 1024;      // 64KB max key
    public static final int MAX_VALUE_LENGTH = 16 * 1024 * 1024;  // 16MB max value

    private BinaryProtocol() {
        // Utility class
    }

    // ==================== Encoding ====================

    /**
     * Encode a command into a ByteBuffer.
     *
     * @param command the command to encode
     * @return ByteBuffer positioned at start, ready to read
     */
    public static ByteBuffer encode(Command command) {
        byte[] keyBytes = command.getKey() != null
            ? command.getKey().getBytes(StandardCharsets.UTF_8)
            : new byte[0];
        byte[] valueBytes = command.getValueUnsafe();
        int valueLen = valueBytes != null ? valueBytes.length : 0;

        int totalSize = REQUEST_HEADER_SIZE + keyBytes.length + valueLen;
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);

        buffer.putInt(MAGIC);
        buffer.put(command.getType());
        buffer.putInt(keyBytes.length);
        buffer.putInt(valueLen);
        buffer.put(keyBytes);
        if (valueBytes != null) {
            buffer.put(valueBytes);
        }

        buffer.flip();
        return buffer;
    }

    /**
     * Encode a command into an existing ByteBuffer.
     *
     * @param command the command to encode
     * @param buffer  the buffer to write to (must have sufficient capacity)
     */
    public static void encode(Command command, ByteBuffer buffer) {
        byte[] keyBytes = command.getKey() != null
            ? command.getKey().getBytes(StandardCharsets.UTF_8)
            : new byte[0];
        byte[] valueBytes = command.getValueUnsafe();
        int valueLen = valueBytes != null ? valueBytes.length : 0;

        buffer.putInt(MAGIC);
        buffer.put(command.getType());
        buffer.putInt(keyBytes.length);
        buffer.putInt(valueLen);
        buffer.put(keyBytes);
        if (valueBytes != null) {
            buffer.put(valueBytes);
        }
    }

    /**
     * Encode a response into a ByteBuffer.
     *
     * @param response the response to encode
     * @return ByteBuffer positioned at start, ready to read
     */
    public static ByteBuffer encode(Response response) {
        byte[] valueBytes = response.getValueUnsafe();
        int valueLen = valueBytes != null ? valueBytes.length : 0;

        int totalSize = RESPONSE_HEADER_SIZE + valueLen;
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);

        buffer.putInt(MAGIC);
        buffer.put(response.getStatus());
        buffer.putInt(valueLen);
        if (valueBytes != null) {
            buffer.put(valueBytes);
        }

        buffer.flip();
        return buffer;
    }

    /**
     * Encode a response into an existing ByteBuffer.
     *
     * @param response the response to encode
     * @param buffer   the buffer to write to
     */
    public static void encode(Response response, ByteBuffer buffer) {
        byte[] valueBytes = response.getValueUnsafe();
        int valueLen = valueBytes != null ? valueBytes.length : 0;

        buffer.putInt(MAGIC);
        buffer.put(response.getStatus());
        buffer.putInt(valueLen);
        if (valueBytes != null) {
            buffer.put(valueBytes);
        }
    }

    // ==================== Decoding ====================

    /**
     * Decode a command from a ByteBuffer.
     *
     * @param buffer the buffer to decode from
     * @return the decoded command
     * @throws ProtocolException if the data is invalid
     */
    public static Command decodeCommand(ByteBuffer buffer) {
        if (buffer.remaining() < REQUEST_HEADER_SIZE) {
            throw new ProtocolException("Incomplete header: need " + REQUEST_HEADER_SIZE +
                " bytes, got " + buffer.remaining());
        }

        int magic = buffer.getInt();
        if (magic != MAGIC) {
            throw new ProtocolException(String.format(
                "Invalid magic bytes: expected 0x%08X, got 0x%08X", MAGIC, magic));
        }

        byte cmdType = buffer.get();
        int keyLength = buffer.getInt();
        int valueLength = buffer.getInt();

        validateLengths(keyLength, valueLength);

        if (buffer.remaining() < keyLength + valueLength) {
            throw new ProtocolException("Incomplete payload: need " + (keyLength + valueLength) +
                " bytes, got " + buffer.remaining());
        }

        String key = null;
        if (keyLength > 0) {
            byte[] keyBytes = new byte[keyLength];
            buffer.get(keyBytes);
            key = new String(keyBytes, StandardCharsets.UTF_8);
        }

        byte[] value = null;
        if (valueLength > 0) {
            value = new byte[valueLength];
            buffer.get(value);
        }

        return new Command(cmdType, key, value);
    }

    /**
     * Decode a response from a ByteBuffer.
     *
     * @param buffer the buffer to decode from
     * @return the decoded response
     * @throws ProtocolException if the data is invalid
     */
    public static Response decodeResponse(ByteBuffer buffer) {
        if (buffer.remaining() < RESPONSE_HEADER_SIZE) {
            throw new ProtocolException("Incomplete header: need " + RESPONSE_HEADER_SIZE +
                " bytes, got " + buffer.remaining());
        }

        int magic = buffer.getInt();
        if (magic != MAGIC) {
            throw new ProtocolException(String.format(
                "Invalid magic bytes: expected 0x%08X, got 0x%08X", MAGIC, magic));
        }

        byte status = buffer.get();
        int valueLength = buffer.getInt();

        if (valueLength < 0 || valueLength > MAX_VALUE_LENGTH) {
            throw new ProtocolException("Invalid value length: " + valueLength);
        }

        if (buffer.remaining() < valueLength) {
            throw new ProtocolException("Incomplete payload: need " + valueLength +
                " bytes, got " + buffer.remaining());
        }

        byte[] value = null;
        if (valueLength > 0) {
            value = new byte[valueLength];
            buffer.get(value);
        }

        return new Response(status, value);
    }

    // ==================== Helpers ====================

    /**
     * Check if a buffer has a complete request.
     *
     * @param buffer the buffer to check
     * @return true if a complete request is available
     */
    public static boolean hasCompleteRequest(ByteBuffer buffer) {
        if (buffer.remaining() < REQUEST_HEADER_SIZE) {
            return false;
        }

        int pos = buffer.position();
        buffer.getInt(); // magic
        buffer.get();    // cmd
        int keyLen = buffer.getInt();
        int valLen = buffer.getInt();
        buffer.position(pos); // restore position

        return buffer.remaining() >= REQUEST_HEADER_SIZE + keyLen + valLen;
    }

    /**
     * Check if a buffer has a complete response.
     *
     * @param buffer the buffer to check
     * @return true if a complete response is available
     */
    public static boolean hasCompleteResponse(ByteBuffer buffer) {
        if (buffer.remaining() < RESPONSE_HEADER_SIZE) {
            return false;
        }

        int pos = buffer.position();
        buffer.getInt(); // magic
        buffer.get();    // status
        int valLen = buffer.getInt();
        buffer.position(pos); // restore position

        return buffer.remaining() >= RESPONSE_HEADER_SIZE + valLen;
    }

    /**
     * Calculate the encoded size of a command.
     *
     * @param command the command
     * @return size in bytes
     */
    public static int encodedSize(Command command) {
        int keyLen = command.getKey() != null
            ? command.getKey().getBytes(StandardCharsets.UTF_8).length
            : 0;
        int valLen = command.getValueUnsafe() != null ? command.getValueUnsafe().length : 0;
        return REQUEST_HEADER_SIZE + keyLen + valLen;
    }

    /**
     * Calculate the encoded size of a response.
     *
     * @param response the response
     * @return size in bytes
     */
    public static int encodedSize(Response response) {
        int valLen = response.getValueUnsafe() != null ? response.getValueUnsafe().length : 0;
        return RESPONSE_HEADER_SIZE + valLen;
    }

    private static void validateLengths(int keyLength, int valueLength) {
        if (keyLength < 0 || keyLength > MAX_KEY_LENGTH) {
            throw new ProtocolException("Invalid key length: " + keyLength);
        }
        if (valueLength < 0 || valueLength > MAX_VALUE_LENGTH) {
            throw new ProtocolException("Invalid value length: " + valueLength);
        }
    }
}
