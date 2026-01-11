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

    private final byte type;
    private final String key;
    private final byte[] value;

    /**
     * Create a new command.
     *
     * @param type  the command type (GET, PUT, DELETE, PING)
     * @param key   the key (may be null for PING)
     * @param value the value (may be null for GET, DELETE, PING)
     */
    public Command(byte type, String key, byte[] value) {
        this.type = type;
        this.key = key;
        this.value = value != null ? Arrays.copyOf(value, value.length) : null;
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
     */
    public boolean hasValue() {
        return value != null && value.length > 0;
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
            default: return "UNKNOWN(" + type + ")";
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Command command = (Command) o;
        return type == command.type &&
               Objects.equals(key, command.key) &&
               Arrays.equals(value, command.value);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(type, key);
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
