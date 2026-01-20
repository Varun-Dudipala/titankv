package com.titankv.network.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Binary wire protocol encoder/decoder for TitanKV.
 *
 * Protocol format:
 * Request:  [MAGIC:4][CMD:1][KEY_LEN:4][VAL_LEN:4][TIMESTAMP:8][EXPIRES:8][KEY:N][VALUE:M]
 * Response: [MAGIC:4][STATUS:1][VAL_LEN:4][TIMESTAMP:8][EXPIRES:8][VALUE:N]
 *
 * Header size: 29 bytes (request), 25 bytes (response)
 */
public final class BinaryProtocol {

    // Magic bytes: "TITK" in hex
    public static final int MAGIC = 0x5449544B;

    // Header sizes
    public static final int REQUEST_HEADER_SIZE = 29;  // magic(4) + cmd(1) + keyLen(4) + valLen(4) + timestamp(8) + expires(8)
    public static final int RESPONSE_HEADER_SIZE = 25;  // magic(4) + status(1) + valLen(4) + timestamp(8) + expires(8)

    // Maximum sizes
    public static final int MAX_KEY_LENGTH = 64 * 1024;      // 64KB max key
    public static final int MAX_VALUE_LENGTH = 16 * 1024 * 1024;  // 16MB max value

    private BinaryProtocol() {
        // Utility class
    }

    // ==================== Encoding ====================

    /**
     * Encode a command into a ByteBuffer.
     * Uses -1 for value length when value is null (distinguishes null from empty array).
     *
     * @param command the command to encode
     * @return ByteBuffer positioned at start, ready to read
     */
    public static ByteBuffer encode(Command command) {
        byte[] keyBytes = command.getKey() != null
            ? command.getKey().getBytes(StandardCharsets.UTF_8)
            : new byte[0];
        byte[] valueBytes = command.getValueUnsafe();
        int valueLen = valueBytes != null ? valueBytes.length : -1;  // -1 = null
        int actualValueLen = valueBytes != null ? valueBytes.length : 0;

        int totalSize = REQUEST_HEADER_SIZE + keyBytes.length + actualValueLen;
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);

        buffer.putInt(MAGIC);
        buffer.put(command.getType());
        buffer.putInt(keyBytes.length);
        buffer.putInt(valueLen);
        buffer.putLong(command.getTimestamp());
        buffer.putLong(command.getExpiresAt());
        buffer.put(keyBytes);
        if (valueBytes != null) {
            buffer.put(valueBytes);
        }

        buffer.flip();
        return buffer;
    }

    /**
     * Encode a command into an existing ByteBuffer.
     * Uses -1 for value length when value is null (distinguishes null from empty array).
     *
     * @param command the command to encode
     * @param buffer  the buffer to write to (must have sufficient capacity)
     */
    public static void encode(Command command, ByteBuffer buffer) {
        byte[] keyBytes = command.getKey() != null
            ? command.getKey().getBytes(StandardCharsets.UTF_8)
            : new byte[0];
        byte[] valueBytes = command.getValueUnsafe();
        int valueLen = valueBytes != null ? valueBytes.length : -1;  // -1 = null

        buffer.putInt(MAGIC);
        buffer.put(command.getType());
        buffer.putInt(keyBytes.length);
        buffer.putInt(valueLen);
        buffer.putLong(command.getTimestamp());
        buffer.putLong(command.getExpiresAt());
        buffer.put(keyBytes);
        if (valueBytes != null) {
            buffer.put(valueBytes);
        }
    }

    /**
     * Encode a response into a ByteBuffer.
     * Uses -1 for value length when value is null (distinguishes null from empty array).
     *
     * @param response the response to encode
     * @return ByteBuffer positioned at start, ready to read
     */
    public static ByteBuffer encode(Response response) {
        byte[] valueBytes = response.getValueUnsafe();
        int valueLen = valueBytes != null ? valueBytes.length : -1;  // -1 = null
        int actualValueLen = valueBytes != null ? valueBytes.length : 0;

        int totalSize = RESPONSE_HEADER_SIZE + actualValueLen;
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);

        buffer.putInt(MAGIC);
        buffer.put(response.getStatus());
        buffer.putInt(valueLen);
        buffer.putLong(response.getTimestamp());
        buffer.putLong(response.getExpiresAt());
        if (valueBytes != null) {
            buffer.put(valueBytes);
        }

        buffer.flip();
        return buffer;
    }

    /**
     * Encode a response into an existing ByteBuffer.
     * Uses -1 for value length when value is null (distinguishes null from empty array).
     *
     * @param response the response to encode
     * @param buffer   the buffer to write to
     */
    public static void encode(Response response, ByteBuffer buffer) {
        byte[] valueBytes = response.getValueUnsafe();
        int valueLen = valueBytes != null ? valueBytes.length : -1;  // -1 = null

        buffer.putInt(MAGIC);
        buffer.put(response.getStatus());
        buffer.putInt(valueLen);
        buffer.putLong(response.getTimestamp());
        buffer.putLong(response.getExpiresAt());
        if (valueBytes != null) {
            buffer.put(valueBytes);
        }
    }

    // ==================== Decoding ====================

    /**
     * Decode a command from a ByteBuffer.
     * Handles -1 value length as null (distinguishes null from empty array).
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
        long timestamp = buffer.getLong();
        long expiresAt = buffer.getLong();

        validateLengths(keyLength, valueLength);

        int actualValueLength = valueLength >= 0 ? valueLength : 0;  // -1 means null
        if (buffer.remaining() < keyLength + actualValueLength) {
            throw new ProtocolException("Incomplete payload: need " + (keyLength + actualValueLength) +
                " bytes, got " + buffer.remaining());
        }

        String key = null;
        if (keyLength > 0) {
            byte[] keyBytes = new byte[keyLength];
            buffer.get(keyBytes);
            key = new String(keyBytes, StandardCharsets.UTF_8);
        }

        byte[] value = null;
        if (valueLength >= 0) {  // -1 = null, 0 = empty array
            value = new byte[valueLength];
            if (valueLength > 0) {
                buffer.get(value);
            }
        }

        return new Command(cmdType, key, value, timestamp, expiresAt);
    }

    /**
     * Decode a response from a ByteBuffer.
     * Handles -1 value length as null (distinguishes null from empty array).
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
        long timestamp = buffer.getLong();
        long expiresAt = buffer.getLong();

        // -1 = null, 0+ = actual length
        if (valueLength < -1 || valueLength > MAX_VALUE_LENGTH) {
            throw new ProtocolException("Invalid value length: " + valueLength);
        }

        int actualValueLength = valueLength >= 0 ? valueLength : 0;
        if (buffer.remaining() < actualValueLength) {
            throw new ProtocolException("Incomplete payload: need " + actualValueLength +
                " bytes, got " + buffer.remaining());
        }

        byte[] value = null;
        if (valueLength >= 0) {  // -1 = null, 0 = empty array
            value = new byte[valueLength];
            if (valueLength > 0) {
                buffer.get(value);
            }
        }

        return new Response(status, value, timestamp, expiresAt);
    }

    // ==================== Helpers ====================

    /**
     * Check if a buffer has a complete request.
     * Handles -1 value length as null (0 bytes in payload).
     * Validates magic bytes and length bounds.
     *
     * SECURITY: Throws ProtocolException on invalid magic bytes (once we have >= 4 bytes)
     * to prevent memory DoS from junk data that never forms a valid request.
     *
     * @param buffer the buffer to check
     * @return true if a complete request is available, false if more bytes needed
     * @throws ProtocolException if magic bytes are invalid or lengths are out of bounds
     */
    public static boolean hasCompleteRequest(ByteBuffer buffer) {
        int remaining = buffer.remaining();
        if (remaining < 4) {
            // Not enough bytes to read magic yet
            return false;
        }

        int pos = buffer.position();

        // Check magic bytes - throw on mismatch to prevent DoS
        int magic = buffer.getInt(pos);
        if (magic != MAGIC) {
            throw new ProtocolException(String.format(
                "Invalid magic bytes: expected 0x%08X, got 0x%08X. " +
                "This may indicate a protocol mismatch or attack.",
                MAGIC, magic));
        }

        if (remaining < REQUEST_HEADER_SIZE) {
            // Valid magic but need more bytes for full header
            return false;
        }

        // cmd at pos+4, keyLen at pos+5, valLen at pos+9
        int keyLen = buffer.getInt(pos + 5);
        int valLen = buffer.getInt(pos + 9);

        // Validate lengths - throw on invalid to prevent DoS
        if (keyLen < 0 || keyLen > MAX_KEY_LENGTH) {
            throw new ProtocolException("Invalid key length: " + keyLen +
                " (must be 0-" + MAX_KEY_LENGTH + ")");
        }
        if (valLen < -1 || valLen > MAX_VALUE_LENGTH) {
            throw new ProtocolException("Invalid value length: " + valLen +
                " (must be -1 to " + MAX_VALUE_LENGTH + ")");
        }

        int actualValLen = valLen >= 0 ? valLen : 0;  // -1 means null
        return remaining >= REQUEST_HEADER_SIZE + keyLen + actualValLen;
    }

    /**
     * Check if a buffer has a complete response.
     * Handles -1 value length as null (0 bytes in payload).
     * Validates magic bytes and length bounds.
     *
     * SECURITY: Throws ProtocolException on invalid magic bytes (once we have >= 4 bytes)
     * to prevent memory DoS from junk data that never forms a valid response.
     *
     * @param buffer the buffer to check
     * @return true if a complete response is available, false if more bytes needed
     * @throws ProtocolException if magic bytes are invalid or value length is out of bounds
     */
    public static boolean hasCompleteResponse(ByteBuffer buffer) {
        int remaining = buffer.remaining();
        if (remaining < 4) {
            // Not enough bytes to read magic yet
            return false;
        }

        int pos = buffer.position();

        // Check magic bytes - throw on mismatch to prevent DoS
        int magic = buffer.getInt(pos);
        if (magic != MAGIC) {
            throw new ProtocolException(String.format(
                "Invalid magic bytes: expected 0x%08X, got 0x%08X. " +
                "This may indicate a protocol mismatch or attack.",
                MAGIC, magic));
        }

        if (remaining < RESPONSE_HEADER_SIZE) {
            // Valid magic but need more bytes for full header
            return false;
        }

        // status at pos+4, valLen at pos+5
        int valLen = buffer.getInt(pos + 5);

        // Validate value length - throw on invalid to prevent DoS
        if (valLen < -1 || valLen > MAX_VALUE_LENGTH) {
            throw new ProtocolException("Invalid value length: " + valLen +
                " (must be -1 to " + MAX_VALUE_LENGTH + ")");
        }

        int actualValLen = valLen >= 0 ? valLen : 0;  // -1 means null
        return remaining >= RESPONSE_HEADER_SIZE + actualValLen;
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
        // -1 is valid (means null), otherwise must be within limits
        if (valueLength < -1 || valueLength > MAX_VALUE_LENGTH) {
            throw new ProtocolException("Invalid value length: " + valueLength);
        }
    }
}
