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

    public ClientConfig() {
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
        this.connectTimeoutMs = connectTimeoutMs;
    }

    public int getReadTimeoutMs() {
        return readTimeoutMs;
    }

    public void setReadTimeoutMs(int readTimeoutMs) {
        this.readTimeoutMs = readTimeoutMs;
    }

    public int getMaxConnectionsPerHost() {
        return maxConnectionsPerHost;
    }

    public void setMaxConnectionsPerHost(int maxConnectionsPerHost) {
        this.maxConnectionsPerHost = maxConnectionsPerHost;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public long getRetryDelayMs() {
        return retryDelayMs;
    }

    public void setRetryDelayMs(long retryDelayMs) {
        this.retryDelayMs = retryDelayMs;
    }

    public boolean isRetryOnFailure() {
        return retryOnFailure;
    }

    public void setRetryOnFailure(boolean retryOnFailure) {
        this.retryOnFailure = retryOnFailure;
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

        public ClientConfig build() {
            return config;
        }
    }
}
