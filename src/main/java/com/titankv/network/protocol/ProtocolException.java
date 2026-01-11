package com.titankv.network.protocol;

/**
 * Exception thrown when protocol encoding/decoding fails.
 */
public class ProtocolException extends RuntimeException {

    public ProtocolException(String message) {
        super(message);
    }

    public ProtocolException(String message, Throwable cause) {
        super(message, cause);
    }
}
