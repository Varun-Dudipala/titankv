package com.titankv.core;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for KeyValuePair to boost core package coverage.
 */
class KeyValuePairTest {

    @Test
    void constructor_copiesValueArray() {
        byte[] original = "test-value".getBytes(StandardCharsets.UTF_8);
        KeyValuePair pair = new KeyValuePair(original, 1000, 0);

        // Modifying original should not affect pair
        original[0] = 'X';

        byte[] retrieved = pair.getValue();
        assertThat(retrieved[0]).isNotEqualTo((byte) 'X');
        assertThat(new String(retrieved, StandardCharsets.UTF_8)).isEqualTo("test-value");
    }

    @Test
    void getValue_returnsCopy() {
        byte[] value = "test-value".getBytes(StandardCharsets.UTF_8);
        KeyValuePair pair = new KeyValuePair(value, 1000, 0);

        byte[] retrieved1 = pair.getValue();
        byte[] retrieved2 = pair.getValue();

        // Should be different instances
        assertThat(retrieved1).isNotSameAs(retrieved2);
        assertThat(retrieved1).isEqualTo(retrieved2);
    }

    @Test
    void getValueUnsafe_returnsSameArray() {
        byte[] value = "test-value".getBytes(StandardCharsets.UTF_8);
        KeyValuePair pair = new KeyValuePair(value, 1000, 0);

        byte[] unsafe1 = pair.getValueUnsafe();
        byte[] unsafe2 = pair.getValueUnsafe();

        // Should be same instance
        assertThat(unsafe1).isSameAs(unsafe2);
    }

    @Test
    void getTimestamp_returnsCorrectValue() {
        long timestamp = System.currentTimeMillis();
        KeyValuePair pair = new KeyValuePair("value".getBytes(), timestamp, 0);

        assertThat(pair.getTimestamp()).isEqualTo(timestamp);
    }

    @Test
    void getExpiresAt_returnsCorrectValue() {
        long expiresAt = System.currentTimeMillis() + 10000;
        KeyValuePair pair = new KeyValuePair("value".getBytes(), 1000, expiresAt);

        assertThat(pair.getExpiresAt()).isEqualTo(expiresAt);
    }

    @Test
    void isExpired_returnsFalse_whenNoExpiration() {
        KeyValuePair pair = new KeyValuePair("value".getBytes(), 1000, 0);

        assertThat(pair.isExpired()).isFalse();
    }

    @Test
    void isExpired_returnsFalse_whenNotYetExpired() {
        long expiresAt = System.currentTimeMillis() + 10000; // 10 seconds in future
        KeyValuePair pair = new KeyValuePair("value".getBytes(), 1000, expiresAt);

        assertThat(pair.isExpired()).isFalse();
    }

    @Test
    void isExpired_returnsTrue_whenExpired() {
        long expiresAt = System.currentTimeMillis() - 1000; // 1 second ago
        KeyValuePair pair = new KeyValuePair("value".getBytes(), 1000, expiresAt);

        assertThat(pair.isExpired()).isTrue();
    }

    @Test
    void isExpired_returnsFalseAtExactExpiration() {
        long now = System.currentTimeMillis();
        KeyValuePair pair = new KeyValuePair("value".getBytes(), 1000, now);

        // At exact expiration time (uses >), not yet expired
        assertThat(pair.isExpired()).isFalse();
    }

    @Test
    void isTombstone_returnsFalse_forRegularValue() {
        KeyValuePair pair = new KeyValuePair("value".getBytes(), 1000, 0);

        assertThat(pair.isTombstone()).isFalse();
    }

    @Test
    void isTombstone_returnsTrue_forNullValue() {
        KeyValuePair pair = new KeyValuePair(null, 1000, 0);

        assertThat(pair.isTombstone()).isTrue();
    }

    @Test
    void isTombstone_returnsFalse_forEmptyArray() {
        KeyValuePair pair = new KeyValuePair(new byte[0], 1000, 0);

        assertThat(pair.isTombstone()).isFalse();
    }

    @Test
    void nullValue_storesAsNull() {
        KeyValuePair pair = new KeyValuePair(null, 1000, 0);

        assertThat(pair.getValue()).isNull();
        assertThat(pair.getValueUnsafe()).isNull();
    }

    @Test
    void emptyValue_storesAsEmptyArray() {
        byte[] empty = new byte[0];
        KeyValuePair pair = new KeyValuePair(empty, 1000, 0);

        assertThat(pair.getValue()).isEmpty();
        assertThat(pair.getValueUnsafe()).isEmpty();
    }

    @Test
    void largeValue_isStored() {
        byte[] large = new byte[1024 * 1024]; // 1 MB
        for (int i = 0; i < large.length; i++) {
            large[i] = (byte) (i % 256);
        }

        KeyValuePair pair = new KeyValuePair(large, 1000, 0);

        assertThat(pair.getValue()).hasSize(1024 * 1024);
        assertThat(pair.getValue()[0]).isEqualTo((byte) 0);
        assertThat(pair.getValue()[255]).isEqualTo((byte) 255);
    }

    @Test
    void zeroTimestamp_isValid() {
        KeyValuePair pair = new KeyValuePair("value".getBytes(), 0, 0);

        assertThat(pair.getTimestamp()).isZero();
    }

    @Test
    void negativeTimestamp_isStored() {
        KeyValuePair pair = new KeyValuePair("value".getBytes(), -1, 0);

        assertThat(pair.getTimestamp()).isEqualTo(-1);
    }

    @Test
    void maxTimestamp_isStored() {
        KeyValuePair pair = new KeyValuePair("value".getBytes(), Long.MAX_VALUE, 0);

        assertThat(pair.getTimestamp()).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void maxExpiresAt_isStored() {
        KeyValuePair pair = new KeyValuePair("value".getBytes(), 1000, Long.MAX_VALUE);

        assertThat(pair.getExpiresAt()).isEqualTo(Long.MAX_VALUE);
        assertThat(pair.isExpired()).isFalse(); // Won't expire for billions of years
    }

    @Test
    void toString_includesBasicInfo() {
        KeyValuePair pair = new KeyValuePair("test".getBytes(), 12345, 67890);

        String str = pair.toString();
        assertThat(str).contains("12345");
        assertThat(str).contains("67890");
    }

    @Test
    void toString_handlesTombstone() {
        KeyValuePair pair = new KeyValuePair(null, 1000, 0);

        String str = pair.toString();
        assertThat(str).contains("valueLength=-1");
        assertThat(str).contains("1000");
    }

    @Test
    void getValue_nullValue_returnsNull() {
        KeyValuePair pair = new KeyValuePair(null, 1000, 0);

        assertThat(pair.getValue()).isNull();
    }

    @Test
    void constructor_nullValue_doesNotThrow() {
        assertThatCode(() -> new KeyValuePair(null, 1000, 0))
            .doesNotThrowAnyException();
    }

    @Test
    void multipleRetrievals_returnConsistentValues() {
        byte[] value = "consistent".getBytes(StandardCharsets.UTF_8);
        KeyValuePair pair = new KeyValuePair(value, 5000, 10000);

        for (int i = 0; i < 10; i++) {
            assertThat(pair.getValue()).isEqualTo(value);
            assertThat(pair.getTimestamp()).isEqualTo(5000);
            assertThat(pair.getExpiresAt()).isEqualTo(10000);
        }
    }

    @Test
    void expirationCheck_multipleCallsConsistent() throws InterruptedException {
        long expiresAt = System.currentTimeMillis() + 100; // 100ms
        KeyValuePair pair = new KeyValuePair("value".getBytes(), 1000, expiresAt);

        assertThat(pair.isExpired()).isFalse();

        Thread.sleep(150); // Wait for expiration

        assertThat(pair.isExpired()).isTrue();
        assertThat(pair.isExpired()).isTrue(); // Still expired
    }

    @Test
    void utf8Value_storedCorrectly() {
        String original = "Hello, 世界! 🌍";
        byte[] bytes = original.getBytes(StandardCharsets.UTF_8);
        KeyValuePair pair = new KeyValuePair(bytes, 1000, 0);

        byte[] retrieved = pair.getValue();
        String decoded = new String(retrieved, StandardCharsets.UTF_8);

        assertThat(decoded).isEqualTo(original);
    }

    @Test
    void binaryValue_storedCorrectly() {
        byte[] binary = new byte[]{0x00, 0x01, (byte) 0xFF, 0x7F, (byte) 0x80};
        KeyValuePair pair = new KeyValuePair(binary, 1000, 0);

        byte[] retrieved = pair.getValue();
        assertThat(retrieved).containsExactly(0x00, 0x01, (byte) 0xFF, 0x7F, (byte) 0x80);
    }
}
