package com.titankv.network;

import com.titankv.TitanKVClient;
import com.titankv.TitanKVServer;
import com.titankv.util.MetricsCollector;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for TCP server with real client connections.
 */
class TcpServerTest {

    private static final int TEST_PORT = 19001;

    private TitanKVServer server;
    private TitanKVClient client;

    @BeforeEach
    void setUp() throws Exception {
        // Start server on test port
        server = new TitanKVServer(TEST_PORT);
        server.start();

        // Wait for server to be ready
        Thread.sleep(100);

        // Create client
        client = new TitanKVClient("localhost:" + TEST_PORT);
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.stop();
        }
    }

    @Test
    void serverStartsAndAcceptsConnections() {
        assertThat(server.isRunning()).isTrue();
        assertThat(client.ping()).isTrue();
    }

    @Test
    void putAndGet_basicOperation() throws IOException {
        client.put("testKey", "testValue");

        Optional<String> result = client.getString("testKey");

        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo("testValue");
    }

    @Test
    void putAndGet_binaryData() throws IOException {
        byte[] binaryValue = new byte[]{0x00, 0x01, 0x02, (byte) 0xFF, (byte) 0xFE};
        client.put("binaryKey", binaryValue);

        Optional<byte[]> result = client.get("binaryKey");

        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(binaryValue);
    }

    @Test
    void get_nonExistentKey_returnsEmpty() throws IOException {
        Optional<byte[]> result = client.get("nonExistentKey");
        assertThat(result).isEmpty();
    }

    @Test
    void delete_existingKey_removesIt() throws IOException {
        client.put("deleteMe", "value");
        assertThat(client.exists("deleteMe")).isTrue();

        client.delete("deleteMe");

        assertThat(client.exists("deleteMe")).isFalse();
        assertThat(client.get("deleteMe")).isEmpty();
    }

    @Test
    void exists_existingKey_returnsTrue() throws IOException {
        client.put("existsKey", "value");
        assertThat(client.exists("existsKey")).isTrue();
    }

    @Test
    void exists_nonExistentKey_returnsFalse() throws IOException {
        assertThat(client.exists("noSuchKey")).isFalse();
    }

    @Test
    void put_overwritesExistingValue() throws IOException {
        client.put("overwriteKey", "value1");
        client.put("overwriteKey", "value2");

        Optional<String> result = client.getString("overwriteKey");
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo("value2");
    }

    @Test
    void largeValue_roundTrip() throws IOException {
        // 32KB value (fits within 64KB buffer, leaving room for headers)
        byte[] largeValue = new byte[32 * 1024];
        for (int i = 0; i < largeValue.length; i++) {
            largeValue[i] = (byte) (i % 256);
        }

        client.put("largeKey", largeValue);
        Optional<byte[]> result = client.get("largeKey");

        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(largeValue);
    }

    @Test
    void unicodeKey_roundTrip() throws IOException {
        String unicodeKey = "key-日本語-emoji-🚀";
        String unicodeValue = "value-中文-한국어-🎉";

        client.put(unicodeKey, unicodeValue);
        Optional<String> result = client.getString(unicodeKey);

        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(unicodeValue);
    }

    @Test
    void concurrentClients_areHandled() throws Exception {
        int numClients = 10;
        int opsPerClient = 100;
        ExecutorService executor = Executors.newFixedThreadPool(numClients);
        CountDownLatch latch = new CountDownLatch(numClients);
        AtomicInteger errors = new AtomicInteger(0);

        for (int c = 0; c < numClients; c++) {
            final int clientId = c;
            executor.submit(() -> {
                try (TitanKVClient threadClient = new TitanKVClient("localhost:" + TEST_PORT)) {
                    for (int i = 0; i < opsPerClient; i++) {
                        String key = "client-" + clientId + "-key-" + i;
                        String value = "value-" + i;
                        threadClient.put(key, value);
                        Optional<String> result = threadClient.getString(key);
                        if (!result.isPresent() || !result.get().equals(value)) {
                            errors.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        assertThat(errors.get()).isEqualTo(0);
    }

    @Test
    void connectionCount_tracksActiveConnections() throws Exception {
        // Initial connection from setUp
        assertThat(server.getConnectionCount()).isGreaterThanOrEqualTo(0);

        // Create additional clients
        TitanKVClient client2 = new TitanKVClient("localhost:" + TEST_PORT);
        client2.ping();
        TitanKVClient client3 = new TitanKVClient("localhost:" + TEST_PORT);
        client3.ping();

        Thread.sleep(100);
        int countWithMultiple = server.getConnectionCount();

        client2.close();
        client3.close();

        // Should have had connections while clients were active
        assertThat(countWithMultiple).isGreaterThan(0);
    }

    @Test
    void metrics_areCollected() throws IOException {
        // Perform some operations
        for (int i = 0; i < 10; i++) {
            client.put("metricsKey" + i, "value" + i);
            client.get("metricsKey" + i);
        }

        MetricsCollector metrics = server.getMetrics();

        assertThat(metrics.getTotalPutOps()).isGreaterThanOrEqualTo(10);
        assertThat(metrics.getTotalGetOps()).isGreaterThanOrEqualTo(10);
    }

    @Test
    void serverStop_closesGracefully() throws Exception {
        // Verify server is running
        assertThat(server.isRunning()).isTrue();
        assertThat(client.ping()).isTrue();

        // Stop server
        server.stop();

        // Server should be stopped
        assertThat(server.isRunning()).isFalse();

        // Client operations should fail
        assertThatThrownBy(() -> client.put("afterStop", "value"))
            .isInstanceOf(IOException.class);
    }
}
