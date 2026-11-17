package com.titankv.network.protocol;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WriteBatchTest {

    private static WriteBatch.Entry entry(String key, String value, long timestamp, long expiresAt) {
        return new WriteBatch.Entry(key, value == null ? null : value.getBytes(StandardCharsets.UTF_8), timestamp,
                expiresAt);
    }

    @Test
    void valuesTombstonesAndEmptyValuesSurviveTheRoundTrip() {
        List<WriteBatch.Entry> writes = List.of(
                entry("user:1", "Ada", 1_000, 0),
                entry("user:2", null, 2_000, 0), // tombstone
                entry("ключ", "", 3_000, 99_999), // an empty value is not a tombstone
                new WriteBatch.Entry("blob", new byte[70_000], 4_000, 0));

        byte[] encoded = WriteBatch.encode(writes);
        List<WriteBatch.Entry> decoded = WriteBatch.decode(encoded);

        assertThat(decoded).hasSize(4);
        for (int i = 0; i < writes.size(); i++) {
            assertThat(decoded.get(i).getKey()).isEqualTo(writes.get(i).getKey());
            assertThat(decoded.get(i).getValue()).isEqualTo(writes.get(i).getValue());
            assertThat(decoded.get(i).getTimestamp()).isEqualTo(writes.get(i).getTimestamp());
            assertThat(decoded.get(i).getExpiresAt()).isEqualTo(writes.get(i).getExpiresAt());
        }
        assertThat(decoded.get(1).getValue()).isNull();
        assertThat(decoded.get(2).getValue()).isEmpty();
        assertThat(encoded.length).isEqualTo(4 + writes.stream().mapToInt(WriteBatch.Entry::encodedSize).sum());
    }

    @Test
    void resultsSurviveTheRoundTrip() {
        long[] results = {0, WriteBatch.REFUSED, 123_456_789_012L};
        assertThat(WriteBatch.decodeResults(WriteBatch.encodeResults(results), 3)).containsExactly(results);
    }

    @Test
    void anAnswerWithTheWrongNumberOfResultsIsRejected() {
        byte[] two = WriteBatch.encodeResults(new long[] {0, 0});
        assertThatThrownBy(() -> WriteBatch.decodeResults(two, 3)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> WriteBatch.decodeResults(null, 1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void emptyAndOversizedBatchesAreNotEncoded() {
        assertThatThrownBy(() -> WriteBatch.encode(List.of())).isInstanceOf(IllegalArgumentException.class);
        List<WriteBatch.Entry> tooMany = new ArrayList<>();
        for (int i = 0; i <= WriteBatch.MAX_ENTRIES; i++) {
            tooMany.add(entry("k" + i, "v", 1, 0));
        }
        assertThatThrownBy(() -> WriteBatch.encode(tooMany)).isInstanceOf(IllegalArgumentException.class);
    }

    /** What arrives is untrusted until the sender has authenticated, and may be damaged after. */
    @Test
    void malformedBatchesAreRejectedWithoutReadingPastTheEnd() {
        byte[] good = WriteBatch.encode(List.of(entry("user:1", "Ada", 1_000, 0), entry("user:2", "Bo", 2_000, 0)));

        assertThatThrownBy(() -> WriteBatch.decode(null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> WriteBatch.decode(new byte[3])).isInstanceOf(IllegalArgumentException.class);
        for (int length = 4; length < good.length; length++) {
            byte[] truncated = Arrays.copyOf(good, length);
            assertThatThrownBy(() -> WriteBatch.decode(truncated)).as("cut to " + length + " bytes")
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> WriteBatch.decode(Arrays.copyOf(good, good.length + 1)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("left over");

        assertThatThrownBy(() -> WriteBatch.decode(withInt(good, 0, 0))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> WriteBatch.decode(withInt(good, 0, -5))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> WriteBatch.decode(withInt(good, 0, Integer.MAX_VALUE)))
                .isInstanceOf(IllegalArgumentException.class);
        // first entry: key length, then value length
        assertThatThrownBy(() -> WriteBatch.decode(withInt(good, 4, Integer.MAX_VALUE)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> WriteBatch.decode(withInt(good, 4, 0))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> WriteBatch.decode(withInt(good, 8, Integer.MAX_VALUE)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> WriteBatch.decode(withInt(good, 8, -2))).isInstanceOf(IllegalArgumentException.class);
    }

    private static byte[] withInt(byte[] bytes, int offset, int value) {
        byte[] copy = bytes.clone();
        ByteBuffer.wrap(copy).putInt(offset, value);
        return copy;
    }
}
