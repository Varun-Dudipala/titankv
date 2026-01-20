package com.titankv.client;

/**
 * Configuration for TitanKV client.
 */
public class ClientConfig {

    private int connectTimeoutMs = 5000;
    private int readTimeoutMs = 30000;
    private int maxConnectionsPerHost = 10;
    private int maxRetries = 3;
    private long retryDelayMs = 100;
    private boolean retryOnFailure = true;
    private String authToken = null;

    public ClientConfig() {
        String token = System.getenv("TITANKV_CLIENT_TOKEN");
        if (token == null || token.isEmpty()) {
            token = System.getProperty("titankv.client.token");
        }
        if (token != null && !token.isEmpty()) {
            this.authToken = token;
        }
    }

    /**
     * Create a config builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    public int getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    public void setConnectTimeoutMs(int connectTimeoutMs) {
        if (connectTimeoutMs <= 0) {
            throw new IllegalArgumentException("connectTimeoutMs must be positive, got: " + connectTimeoutMs);
        }
        this.connectTimeoutMs = connectTimeoutMs;
    }

    public int getReadTimeoutMs() {
        return readTimeoutMs;
    }

    public void setReadTimeoutMs(int readTimeoutMs) {
        if (readTimeoutMs <= 0) {
            throw new IllegalArgumentException("readTimeoutMs must be positive, got: " + readTimeoutMs);
        }
        this.readTimeoutMs = readTimeoutMs;
    }

    public int getMaxConnectionsPerHost() {
        return maxConnectionsPerHost;
    }

    public void setMaxConnectionsPerHost(int maxConnectionsPerHost) {
        if (maxConnectionsPerHost <= 0) {
            throw new IllegalArgumentException("maxConnectionsPerHost must be positive, got: " + maxConnectionsPerHost);
        }
        this.maxConnectionsPerHost = maxConnectionsPerHost;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must be non-negative, got: " + maxRetries);
        }
        this.maxRetries = maxRetries;
    }

    public long getRetryDelayMs() {
        return retryDelayMs;
    }

    public void setRetryDelayMs(long retryDelayMs) {
        if (retryDelayMs < 0) {
            throw new IllegalArgumentException("retryDelayMs must be non-negative, got: " + retryDelayMs);
        }
        this.retryDelayMs = retryDelayMs;
    }

    public boolean isRetryOnFailure() {
        return retryOnFailure;
    }

    public void setRetryOnFailure(boolean retryOnFailure) {
        this.retryOnFailure = retryOnFailure;
    }

    public String getAuthToken() {
        return authToken;
    }

    public void setAuthToken(String authToken) {
        this.authToken = authToken;
    }

    /**
     * Builder for ClientConfig.
     */
    public static class Builder {
        private final ClientConfig config = new ClientConfig();

        public Builder connectTimeoutMs(int timeout) {
            config.setConnectTimeoutMs(timeout);
            return this;
        }

        public Builder readTimeoutMs(int timeout) {
            config.setReadTimeoutMs(timeout);
            return this;
        }

        public Builder maxConnectionsPerHost(int max) {
            config.setMaxConnectionsPerHost(max);
            return this;
        }

        public Builder maxRetries(int max) {
            config.setMaxRetries(max);
            return this;
        }

        public Builder retryDelayMs(long delay) {
            config.setRetryDelayMs(delay);
            return this;
        }

        public Builder retryOnFailure(boolean retry) {
            config.setRetryOnFailure(retry);
            return this;
        }

        public Builder authToken(String token) {
            config.setAuthToken(token);
            return this;
        }

        public ClientConfig build() {
            return config;
        }
    }
}
