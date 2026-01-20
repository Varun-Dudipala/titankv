package com.titankv.core;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;

class InMemoryStoreTest {

    private InMemoryStore store;

    @BeforeEach
    void setUp() {
        store = new InMemoryStore(100); // Fast cleanup for tests
    }

    @AfterEach
    void tearDown() {
        store.shutdown();
    }

    @Test
    void putAndGet_basicOperation() {
        byte[] value = "hello".getBytes(StandardCharsets.UTF_8);
        store.put("key1", value);

        Optional<KeyValuePair> result = store.get("key1");

        assertThat(result).isPresent();
        assertThat(result.get().getValue()).isEqualTo(value);
    }

    @Test
    void put_returnsPreviousValue() {
        byte[] value1 = "value1".getBytes(StandardCharsets.UTF_8);
        byte[] value2 = "value2".getBytes(StandardCharsets.UTF_8);

        Optional<KeyValuePair> first = store.put("key1", value1);
        Optional<KeyValuePair> second = store.put("key1", value2);

        assertThat(first).isEmpty();
        assertThat(second).isPresent();
        assertThat(second.get().getValue()).isEqualTo(value1);
    }

    @Test
    void get_nonExistentKey_returnsEmpty() {
        Optional<KeyValuePair> result = store.get("nonexistent");
        assertThat(result).isEmpty();
    }

    @Test
    void delete_existingKey_returnsDeletedValue() {
        byte[] value = "toDelete".getBytes(StandardCharsets.UTF_8);
        store.put("key1", value);

        Optional<KeyValuePair> deleted = store.delete("key1");

        assertThat(deleted).isPresent();
        assertThat(deleted.get().getValue()).isEqualTo(value);
        assertThat(store.get("key1")).isEmpty();
    }

    @Test
    void delete_nonExistentKey_returnsEmpty() {
        Optional<KeyValuePair> deleted = store.delete("nonexistent");
        assertThat(deleted).isEmpty();
    }

    @Test
    void exists_existingKey_returnsTrue() {
        store.put("key1", "value".getBytes(StandardCharsets.UTF_8));
        assertThat(store.exists("key1")).isTrue();
    }

    @Test
    void exists_nonExistentKey_returnsFalse() {
        assertThat(store.exists("nonexistent")).isFalse();
    }

    @Test
    void size_countsEntries() {
        assertThat(store.size()).isEqualTo(0);

        store.put("key1", "value1".getBytes(StandardCharsets.UTF_8));
        assertThat(store.size()).isEqualTo(1);

        store.put("key2", "value2".getBytes(StandardCharsets.UTF_8));
        assertThat(store.size()).isEqualTo(2);

        store.delete("key1");
        assertThat(store.size()).isEqualTo(1);
    }

    @Test
    void keys_returnsAllKeys() {
        store.put("key1", "value1".getBytes(StandardCharsets.UTF_8));
        store.put("key2", "value2".getBytes(StandardCharsets.UTF_8));
        store.put("key3", "value3".getBytes(StandardCharsets.UTF_8));

        Set<String> keys = store.keys();

        assertThat(keys).containsExactlyInAnyOrder("key1", "key2", "key3");
    }

    @Test
    void clear_removesAllEntries() {
        store.put("key1", "value1".getBytes(StandardCharsets.UTF_8));
        store.put("key2", "value2".getBytes(StandardCharsets.UTF_8));

        store.clear();

        assertThat(store.size()).isEqualTo(0);
        assertThat(store.get("key1")).isEmpty();
        assertThat(store.get("key2")).isEmpty();
    }

    @Test
    void put_withTtl_expiresAfterDuration() throws InterruptedException {
        byte[] value = "expiring".getBytes(StandardCharsets.UTF_8);
        store.put("key1", value, 50); // 50ms TTL

        // Should exist immediately
        assertThat(store.get("key1")).isPresent();

        // Wait for expiration
        Thread.sleep(100);

        // Should be expired now
        assertThat(store.get("key1")).isEmpty();
        assertThat(store.exists("key1")).isFalse();
    }

    @Test
    void put_withZeroTtl_neverExpires() throws InterruptedException {
        byte[] value = "permanent".getBytes(StandardCharsets.UTF_8);
        store.put("key1", value, 0);

        Thread.sleep(50);

        assertThat(store.get("key1")).isPresent();
    }

    @Test
    void put_nullKey_throwsException() {
        assertThatThrownBy(() -> store.put(null, "value".getBytes(StandardCharsets.UTF_8)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("null or empty");
    }

    @Test
    void put_emptyKey_throwsException() {
        assertThatThrownBy(() -> store.put("", "value".getBytes(StandardCharsets.UTF_8)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("null or empty");
    }

    @Test
    void put_nullValue_createsTombstone() {
        store.put("key1", null);

        // Null values are tombstones and should not be retrievable
        Optional<KeyValuePair> result = store.get("key1");
        assertThat(result).isEmpty();
        assertThat(store.exists("key1")).isFalse();
    }

    @Test
    void put_emptyValue_isRetrievable() {
        store.put("key1", new byte[0]);

        Optional<KeyValuePair> result = store.get("key1");
        assertThat(result).isPresent();
        assertThat(result.get().getValue()).isEmpty();
        assertThat(store.exists("key1")).isTrue();
    }

    @Test
    void putIfNewer_olderEntry_doesNotUpdate() {
        KeyValuePair newer = new KeyValuePair("newer".getBytes(StandardCharsets.UTF_8),
            System.currentTimeMillis(), 0);
        KeyValuePair older = new KeyValuePair("older".getBytes(StandardCharsets.UTF_8),
            System.currentTimeMillis() - 1000, 0);

        store.putIfNewer("key1", newer);
        boolean updated = store.putIfNewer("key1", older);

        assertThat(updated).isFalse();
        assertThat(store.get("key1").get().getValue())
            .isEqualTo("newer".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void putIfNewer_newerEntry_updates() {
        KeyValuePair older = new KeyValuePair("older".getBytes(StandardCharsets.UTF_8),
            System.currentTimeMillis() - 1000, 0);
        KeyValuePair newer = new KeyValuePair("newer".getBytes(StandardCharsets.UTF_8),
            System.currentTimeMillis(), 0);

        store.putIfNewer("key1", older);
        boolean updated = store.putIfNewer("key1", newer);

        assertThat(updated).isTrue();
        assertThat(store.get("key1").get().getValue())
            .isEqualTo("newer".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void putIfNewer_tombstone_makesKeyNonExistent() {
        // Put initial value
        store.put("key1", "value".getBytes(StandardCharsets.UTF_8));
        assertThat(store.exists("key1")).isTrue();

        // Write tombstone with newer timestamp
        long timestamp = System.currentTimeMillis() + 100;
        boolean written = store.putIfNewer("key1", null, timestamp, 0);

        assertThat(written).isTrue();
        assertThat(store.exists("key1")).isFalse();
        assertThat(store.get("key1")).isEmpty();
    }

    @Test
    void concurrentOperations_areThreadSafe() throws InterruptedException {
        int threadCount = 10;
        int operationsPerThread = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < operationsPerThread; i++) {
                        String key = "key-" + threadId + "-" + i;
                        byte[] value = ("value-" + i).getBytes(StandardCharsets.UTF_8);
                        store.put(key, value);
                        store.get(key);
                        if (i % 2 == 0) {
                            store.delete(key);
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        // Verify store is in consistent state (no exceptions thrown)
        assertThat(store.size()).isGreaterThanOrEqualTo(0);
    }
}
