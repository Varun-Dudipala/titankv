package com.titankv.network.protocol;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * The payload of {@link Command#PUT_BATCH_INTERNAL}: several replica writes in one request, and
 * the replica's answer to each.
 *
 * <pre>
 * Request value:  [COUNT:4] then per write [KEY_LEN:4][VAL_LEN:4][TIMESTAMP:8][EXPIRES:8][KEY][VALUE]
 * Response value: per write [RESULT:8]
 * </pre>
 *
 * VAL_LEN is -1 for a tombstone. RESULT is 0 if the replica stored that version, the newer
 * version it kept instead, or {@link #REFUSED} if it could not take the write.
 */
public final class WriteBatch {

    /** The result of a write the replica could not apply (a full store, a failed disk). */
    public static final long REFUSED = -1;
    public static final int MAX_ENTRIES = 4096;
    private static final int ENTRY_HEADER = 4 + 4 + 8 + 8;

    private WriteBatch() {
    }

    /** One replica write: a versioned value, or a tombstone if the value is null. */
    public static final class Entry {
        private final String key;
        private final byte[] value;
        private final long timestamp;
        private final long expiresAt;

        public Entry(String key, byte[] value, long timestamp, long expiresAt) {
            this.key = key;
            this.value = value;
            this.timestamp = timestamp;
            this.expiresAt = expiresAt;
        }

        public String getKey() {
            return key;
        }

        /** @return the value, or null for a tombstone. Not copied. */
        public byte[] getValue() {
            return value;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public long getExpiresAt() {
            return expiresAt;
        }

        /** Bytes this entry takes in an encoded batch. */
        public int encodedSize() {
            return ENTRY_HEADER + key.getBytes(StandardCharsets.UTF_8).length + (value != null ? value.length : 0);
        }
    }

    public static byte[] encode(List<Entry> entries) {
        if (entries.isEmpty() || entries.size() > MAX_ENTRIES) {
            throw new IllegalArgumentException("A batch holds 1 to " + MAX_ENTRIES + " writes, not " + entries.size());
        }
        byte[][] keys = new byte[entries.size()][];
        int size = 4;
        for (int i = 0; i < keys.length; i++) {
            Entry entry = entries.get(i);
            keys[i] = entry.key.getBytes(StandardCharsets.UTF_8);
            size += ENTRY_HEADER + keys[i].length + (entry.value != null ? entry.value.length : 0);
        }
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.putInt(keys.length);
        for (int i = 0; i < keys.length; i++) {
            Entry entry = entries.get(i);
            buffer.putInt(keys[i].length);
            buffer.putInt(entry.value != null ? entry.value.length : -1);
            buffer.putLong(entry.timestamp);
            buffer.putLong(entry.expiresAt);
            buffer.put(keys[i]);
            if (entry.value != null) {
                buffer.put(entry.value);
            }
        }
        return buffer.array();
    }

    /**
     * @throws IllegalArgumentException if the bytes are not a well-formed batch
     */
    public static List<Entry> decode(byte[] payload) {
        if (payload == null || payload.length < 4) {
            throw new IllegalArgumentException("Empty write batch");
        }
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        int count = buffer.getInt();
        if (count < 1 || count > MAX_ENTRIES) {
            throw new IllegalArgumentException("Invalid write batch: " + count + " writes");
        }
        List<Entry> entries = new ArrayList<>(count);
        try {
            for (int i = 0; i < count; i++) {
                int keyLength = buffer.getInt();
                int valueLength = buffer.getInt();
                long timestamp = buffer.getLong();
                long expiresAt = buffer.getLong();
                if (keyLength < 1 || keyLength > BinaryProtocol.MAX_KEY_LENGTH || valueLength < -1
                        || keyLength > buffer.remaining()
                        || Math.max(valueLength, 0) > buffer.remaining() - keyLength) {
                    throw new IllegalArgumentException("Invalid write batch: lengths of write " + i);
                }
                byte[] key = new byte[keyLength];
                buffer.get(key);
                byte[] value = null;
                if (valueLength >= 0) {
                    value = new byte[valueLength];
                    buffer.get(value);
                }
                entries.add(new Entry(new String(key, StandardCharsets.UTF_8), value, timestamp, expiresAt));
            }
        } catch (BufferUnderflowException e) {
            throw new IllegalArgumentException("Invalid write batch: truncated", e);
        }
        if (buffer.hasRemaining()) {
            throw new IllegalArgumentException("Invalid write batch: " + buffer.remaining() + " bytes left over");
        }
        return entries;
    }

    public static byte[] encodeResults(long[] results) {
        ByteBuffer buffer = ByteBuffer.allocate(results.length * 8);
        for (long result : results) {
            buffer.putLong(result);
        }
        return buffer.array();
    }

    /**
     * @throws IllegalArgumentException if the answer does not hold one result per write sent
     */
    public static long[] decodeResults(byte[] payload, int expected) {
        if (payload == null || payload.length != expected * 8) {
            throw new IllegalArgumentException("Expected " + expected + " results, got "
                    + (payload == null ? 0 : payload.length) + " bytes");
        }
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        long[] results = new long[expected];
        for (int i = 0; i < expected; i++) {
            results[i] = buffer.getLong();
        }
        return results;
    }
}
