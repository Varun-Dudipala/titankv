package com.titankv.network.protocol;

import java.util.Arrays;
import java.util.Objects;

/**
 * Immutable response object representing a server response.
 */
public final class Response {

    // Response status codes
    public static final byte OK = 0x00;
    public static final byte NOT_FOUND = 0x01;
    public static final byte ERROR = 0x02;
    public static final byte PONG = 0x03;
    public static final byte EXISTS_TRUE = 0x04;
    public static final byte EXISTS_FALSE = 0x05;

    private final byte status;
    private final byte[] value;
    private final String errorMessage;

    /**
     * Create a new response.
     *
     * @param status the response status code
     * @param value  the value (may be null)
     */
    public Response(byte status, byte[] value) {
        this(status, value, null);
    }

    /**
     * Create a new response with error message.
     *
     * @param status       the response status code
     * @param value        the value (may be null)
     * @param errorMessage the error message (for ERROR status)
     */
    public Response(byte status, byte[] value, String errorMessage) {
        this.status = status;
        this.value = value != null ? Arrays.copyOf(value, value.length) : null;
        this.errorMessage = errorMessage;
    }

    /**
     * Create a successful response with value.
     */
    public static Response ok(byte[] value) {
        return new Response(OK, value);
    }

    /**
     * Create a successful response with no value.
     */
    public static Response ok() {
        return new Response(OK, null);
    }

    /**
     * Create a not found response.
     */
    public static Response notFound() {
        return new Response(NOT_FOUND, null);
    }

    /**
     * Create an error response.
     */
    public static Response error(String message) {
        byte[] msgBytes = message != null ? message.getBytes(java.nio.charset.StandardCharsets.UTF_8) : null;
        return new Response(ERROR, msgBytes, message);
    }

    /**
     * Create a PONG response.
     */
    public static Response pong() {
        return new Response(PONG, null);
    }

    /**
     * Create an EXISTS response.
     */
    public static Response exists(boolean exists) {
        return new Response(exists ? EXISTS_TRUE : EXISTS_FALSE, null);
    }

    /**
     * Get the response status.
     *
     * @return status code
     */
    public byte getStatus() {
        return status;
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
     * Get the error message.
     *
     * @return error message, or null if not an error
     */
    public String getErrorMessage() {
        if (errorMessage != null) {
            return errorMessage;
        }
        if (status == ERROR && value != null) {
            return new String(value, java.nio.charset.StandardCharsets.UTF_8);
        }
        return null;
    }

    /**
     * Check if this is a successful response.
     */
    public boolean isOk() {
        return status == OK || status == PONG || status == EXISTS_TRUE;
    }

    /**
     * Check if this is an error response.
     */
    public boolean isError() {
        return status == ERROR;
    }

    /**
     * Check if this is a not found response.
     */
    public boolean isNotFound() {
        return status == NOT_FOUND;
    }

    /**
     * Check if this response has a value.
     */
    public boolean hasValue() {
        return value != null && value.length > 0;
    }

    /**
     * Get a human-readable status name.
     */
    public String getStatusName() {
        switch (status) {
            case OK: return "OK";
            case NOT_FOUND: return "NOT_FOUND";
            case ERROR: return "ERROR";
            case PONG: return "PONG";
            case EXISTS_TRUE: return "EXISTS_TRUE";
            case EXISTS_FALSE: return "EXISTS_FALSE";
            default: return "UNKNOWN(" + status + ")";
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Response response = (Response) o;
        return status == response.status &&
               Arrays.equals(value, response.value);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(status);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }

    @Override
    public String toString() {
        return "Response{" +
               "status=" + getStatusName() +
               ", valueLength=" + (value != null ? value.length : 0) +
               (errorMessage != null ? ", error='" + errorMessage + "'" : "") +
               '}';
    }
}
