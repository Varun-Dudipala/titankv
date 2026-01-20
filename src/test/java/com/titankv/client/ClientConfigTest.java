package com.titankv.client;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for ClientConfig to boost client package coverage.
 */
class ClientConfigTest {

    @Test
    void builder_defaultValues() {
        ClientConfig config = ClientConfig.builder().build();

        assertThat(config.getConnectTimeoutMs()).isEqualTo(5000);
        assertThat(config.getReadTimeoutMs()).isEqualTo(30000);
        assertThat(config.getMaxConnectionsPerHost()).isEqualTo(10);
        assertThat(config.isRetryOnFailure()).isTrue();
        assertThat(config.getMaxRetries()).isEqualTo(3);
        assertThat(config.getAuthToken()).isNull();
    }

    @Test
    void builder_customConnectTimeout() {
        ClientConfig config = ClientConfig.builder()
            .connectTimeoutMs(3000)
            .build();

        assertThat(config.getConnectTimeoutMs()).isEqualTo(3000);
    }

    @Test
    void builder_customReadTimeout() {
        ClientConfig config = ClientConfig.builder()
            .readTimeoutMs(15000)
            .build();

        assertThat(config.getReadTimeoutMs()).isEqualTo(15000);
    }

    @Test
    void builder_customMaxConnections() {
        ClientConfig config = ClientConfig.builder()
            .maxConnectionsPerHost(20)
            .build();

        assertThat(config.getMaxConnectionsPerHost()).isEqualTo(20);
    }

    @Test
    void builder_disableRetry() {
        ClientConfig config = ClientConfig.builder()
            .retryOnFailure(false)
            .build();

        assertThat(config.isRetryOnFailure()).isFalse();
    }

    @Test
    void builder_customMaxRetries() {
        ClientConfig config = ClientConfig.builder()
            .maxRetries(5)
            .build();

        assertThat(config.getMaxRetries()).isEqualTo(5);
    }

    @Test
    void builder_withAuthToken() {
        ClientConfig config = ClientConfig.builder()
            .authToken("my-secret-token")
            .build();

        assertThat(config.getAuthToken()).isEqualTo("my-secret-token");
    }

    @Test
    void builder_allCustomValues() {
        ClientConfig config = ClientConfig.builder()
            .connectTimeoutMs(2000)
            .readTimeoutMs(8000)
            .maxConnectionsPerHost(15)
            .retryOnFailure(false)
            .maxRetries(2)
            .authToken("custom-token")
            .build();

        assertThat(config.getConnectTimeoutMs()).isEqualTo(2000);
        assertThat(config.getReadTimeoutMs()).isEqualTo(8000);
        assertThat(config.getMaxConnectionsPerHost()).isEqualTo(15);
        assertThat(config.isRetryOnFailure()).isFalse();
        assertThat(config.getMaxRetries()).isEqualTo(2);
        assertThat(config.getAuthToken()).isEqualTo("custom-token");
    }

    @Test
    void builder_zeroTimeout_throws() {
        assertThatThrownBy(() ->
            ClientConfig.builder()
                .connectTimeoutMs(0)
                .build()
        ).isInstanceOf(IllegalArgumentException.class)
         .hasMessageContaining("connectTimeoutMs must be positive");
    }

    @Test
    void builder_negativeTimeouts_throws() {
        assertThatThrownBy(() ->
            ClientConfig.builder()
                .connectTimeoutMs(-1)
                .build()
        ).isInstanceOf(IllegalArgumentException.class)
         .hasMessageContaining("connectTimeoutMs must be positive");
    }

    @Test
    void builder_singleConnection() {
        ClientConfig config = ClientConfig.builder()
            .maxConnectionsPerHost(1)
            .build();

        assertThat(config.getMaxConnectionsPerHost()).isEqualTo(1);
    }

    @Test
    void builder_veryLargeConnectionPool() {
        ClientConfig config = ClientConfig.builder()
            .maxConnectionsPerHost(1000)
            .build();

        assertThat(config.getMaxConnectionsPerHost()).isEqualTo(1000);
    }

    @Test
    void builder_emptyAuthToken() {
        ClientConfig config = ClientConfig.builder()
            .authToken("")
            .build();

        assertThat(config.getAuthToken()).isEmpty();
    }

    @Test
    void builder_nullAuthToken() {
        ClientConfig config = ClientConfig.builder()
            .authToken(null)
            .build();

        assertThat(config.getAuthToken()).isNull();
    }

    @Test
    void builder_zeroRetries() {
        ClientConfig config = ClientConfig.builder()
            .maxRetries(0)
            .build();

        assertThat(config.getMaxRetries()).isZero();
    }

    @Test
    void builder_chainingMethods() {
        ClientConfig config = ClientConfig.builder()
            .connectTimeoutMs(1000)
            .readTimeoutMs(2000)
            .maxConnectionsPerHost(5)
            .retryOnFailure(true)
            .maxRetries(4)
            .authToken("token123")
            .build();

        assertThat(config).isNotNull();
        assertThat(config.getConnectTimeoutMs()).isEqualTo(1000);
        assertThat(config.getReadTimeoutMs()).isEqualTo(2000);
        assertThat(config.getMaxConnectionsPerHost()).isEqualTo(5);
        assertThat(config.isRetryOnFailure()).isTrue();
        assertThat(config.getMaxRetries()).isEqualTo(4);
        assertThat(config.getAuthToken()).isEqualTo("token123");
    }

    @Test
    void builder_multipleBuilds() {
        ClientConfig.Builder builder = ClientConfig.builder()
            .connectTimeoutMs(3000);

        ClientConfig config1 = builder.build();
        ClientConfig config2 = builder.build();

        // Builder may return same instance (implementation detail)
        assertThat(config1.getConnectTimeoutMs()).isEqualTo(config2.getConnectTimeoutMs());
    }

    @Test
    void builder_overwriteValues() {
        ClientConfig config = ClientConfig.builder()
            .connectTimeoutMs(1000)
            .connectTimeoutMs(2000) // Overwrite
            .build();

        assertThat(config.getConnectTimeoutMs()).isEqualTo(2000);
    }

    @Test
    void builder_toString_notNull() {
        ClientConfig config = ClientConfig.builder()
            .connectTimeoutMs(5000)
            .readTimeoutMs(10000)
            .build();

        String str = config.toString();
        assertThat(str).isNotNull();
        assertThat(str).isNotEmpty();
    }
}
