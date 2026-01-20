package com.titankv.network.protocol;

import java.util.Arrays;
import java.util.Objects;

/**
 * Immutable command object representing a client request.
 */
public final class Command {

    // Command types
    public static final byte GET = 0x01;
    public static final byte PUT = 0x02;
    public static final byte DELETE = 0x03;
    public static final byte PING = 0x04;
    public static final byte EXISTS = 0x05;
    public static final byte KEYS = 0x06;
    public static final byte AUTH = 0x07;
    // Internal commands (not exposed to clients)
    public static final byte GET_INTERNAL = 0x11;  // Get from local store only
    public static final byte PUT_INTERNAL = 0x12;  // Put without triggering replication
    public static final byte DELETE_INTERNAL = 0x13;  // Delete without triggering replication

    private final byte type;
    private final String key;
    private final byte[] value;
    private final long timestamp;   // Server-side timestamp for versioning (0 = not set)
    private final long expiresAt;   // Expiration timestamp (0 = no expiration)

    /**
     * Create a new command with timestamp and expiry metadata.
     *
     * @param type      the command type (GET, PUT, DELETE, PING)
     * @param key       the key (may be null for PING)
     * @param value     the value (may be null for GET, DELETE, PING)
     * @param timestamp server-side timestamp for versioning (0 = assign on store)
     * @param expiresAt expiration timestamp (0 = no expiration)
     */
    public Command(byte type, String key, byte[] value, long timestamp, long expiresAt) {
        this.type = type;
        this.key = key;
        this.value = value != null ? Arrays.copyOf(value, value.length) : null;
        this.timestamp = timestamp;
        this.expiresAt = expiresAt;
    }

    /**
     * Create a new command without timestamp/expiry (for compatibility).
     *
     * @param type  the command type (GET, PUT, DELETE, PING)
     * @param key   the key (may be null for PING)
     * @param value the value (may be null for GET, DELETE, PING)
     */
    public Command(byte type, String key, byte[] value) {
        this(type, key, value, 0, 0);
    }

    /**
     * Create a GET command.
     */
    public static Command get(String key) {
        return new Command(GET, key, null);
    }

    /**
     * Create a PUT command.
     */
    public static Command put(String key, byte[] value) {
        return new Command(PUT, key, value);
    }

    /**
     * Create a DELETE command.
     */
    public static Command delete(String key) {
        return new Command(DELETE, key, null);
    }

    /**
     * Create a PING command.
     */
    public static Command ping() {
        return new Command(PING, null, null);
    }

    /**
     * Create an EXISTS command.
     */
    public static Command exists(String key) {
        return new Command(EXISTS, key, null);
    }

    /**
     * Create an AUTH command.
     *
     * @param token authentication token
     */
    public static Command auth(String token) {
        byte[] tokenBytes = token != null ? token.getBytes(java.nio.charset.StandardCharsets.UTF_8) : null;
        return new Command(AUTH, null, tokenBytes);
    }

    /**
     * Get the command type.
     *
     * @return command type byte
     */
    public byte getType() {
        return type;
    }

    /**
     * Get the key.
     *
     * @return the key, or null for PING
     */
    public String getKey() {
        return key;
    }

    /**
     * Get the value.
     *
     * @return copy of the value, or null if no value
     */
    public byte[] getValue() {
        return value != null ? Arrays.copyOf(value, value.length) : null;
    }

    /**
     * Get the raw value without copying.
     *
     * @return internal value array, or null
     */
    public byte[] getValueUnsafe() {
        return value;
    }

    /**
     * Check if this command has a value.
     * Returns true for both non-null values and empty arrays (distinguishes null from empty).
     */
    public boolean hasValue() {
        return value != null;
    }

    /**
     * Get the timestamp for versioning.
     *
     * @return timestamp in milliseconds since epoch, or 0 if not set
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Get the expiration timestamp.
     *
     * @return expiration timestamp in milliseconds since epoch, or 0 for no expiration
     */
    public long getExpiresAt() {
        return expiresAt;
    }

    /**
     * Get a human-readable command type name.
     */
    public String getTypeName() {
        switch (type) {
            case GET: return "GET";
            case PUT: return "PUT";
            case DELETE: return "DELETE";
            case PING: return "PING";
            case EXISTS: return "EXISTS";
            case KEYS: return "KEYS";
            case AUTH: return "AUTH";
            default: return "UNKNOWN(" + type + ")";
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Command command = (Command) o;
        return type == command.type &&
               timestamp == command.timestamp &&
               expiresAt == command.expiresAt &&
               Objects.equals(key, command.key) &&
               Arrays.equals(value, command.value);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(type, key, timestamp, expiresAt);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }

    @Override
    public String toString() {
        return "Command{" +
               "type=" + getTypeName() +
               ", key='" + key + '\'' +
               ", valueLength=" + (value != null ? value.length : 0) +
               '}';
    }
}
