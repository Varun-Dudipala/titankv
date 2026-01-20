package com.titankv.consistency;

/**
 * Exception thrown when a consistency requirement cannot be met.
 */
public class ConsistencyException extends RuntimeException {

    private final ConsistencyLevel requestedLevel;
    private final int requiredResponses;
    private final int actualResponses;

    public ConsistencyException(String message) {
        super(message);
        this.requestedLevel = null;
        this.requiredResponses = 0;
        this.actualResponses = 0;
    }

    public ConsistencyException(String message, ConsistencyLevel level, int required, int actual) {
        super(String.format("%s (level=%s, required=%d, actual=%d)",
            message, level, required, actual));
        this.requestedLevel = level;
        this.requiredResponses = required;
        this.actualResponses = actual;
    }

    public ConsistencyException(String message, int required, int actual) {
        super(String.format("%s (required=%d, actual=%d)", message, required, actual));
        this.requestedLevel = null;
        this.requiredResponses = required;
        this.actualResponses = actual;
    }

    public ConsistencyException(String message, Throwable cause) {
        super(message, cause);
        this.requestedLevel = null;
        this.requiredResponses = 0;
        this.actualResponses = 0;
    }

    public ConsistencyLevel getRequestedLevel() {
        return requestedLevel;
    }

    public int getRequiredResponses() {
        return requiredResponses;
    }

    public int getActualResponses() {
        return actualResponses;
    }
}
