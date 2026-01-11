package com.titankv.util;

/**
 * MurmurHash3 implementation for 64-bit hashing.
 * Fast, non-cryptographic hash with excellent distribution.
 * Used for consistent hashing ring placement.
 */
public final class MurmurHash3 {

    private static final long C1 = 0x87c37b91114253d5L;
    private static final long C2 = 0x4cf5ad432745937fL;
    private static final int SEED = 0x9747b28c;

    private MurmurHash3() {
        // Utility class
    }

    /**
     * Generate a 64-bit hash from a byte array.
     *
     * @param data the data to hash
     * @return 64-bit hash value
     */
    public static long hash64(byte[] data) {
        return hash64(data, 0, data.length, SEED);
    }

    /**
     * Generate a 64-bit hash from a byte array with custom seed.
     *
     * @param data the data to hash
     * @param seed the seed value
     * @return 64-bit hash value
     */
    public static long hash64(byte[] data, int seed) {
        return hash64(data, 0, data.length, seed);
    }

    /**
     * Generate a 64-bit hash from a portion of a byte array.
     *
     * @param data   the data to hash
     * @param offset starting position in the array
     * @param length number of bytes to hash
     * @param seed   the seed value
     * @return 64-bit hash value
     */
    public static long hash64(byte[] data, int offset, int length, int seed) {
        long h1 = seed & 0x00000000FFFFFFFFL;
        long h2 = seed & 0x00000000FFFFFFFFL;

        int nblocks = length / 16;

        // Body
        for (int i = 0; i < nblocks; i++) {
            int idx = offset + (i * 16);
            long k1 = getLongLittleEndian(data, idx);
            long k2 = getLongLittleEndian(data, idx + 8);

            // Mix k1
            k1 *= C1;
            k1 = Long.rotateLeft(k1, 31);
            k1 *= C2;
            h1 ^= k1;
            h1 = Long.rotateLeft(h1, 27);
            h1 += h2;
            h1 = h1 * 5 + 0x52dce729;

            // Mix k2
            k2 *= C2;
            k2 = Long.rotateLeft(k2, 33);
            k2 *= C1;
            h2 ^= k2;
            h2 = Long.rotateLeft(h2, 31);
            h2 += h1;
            h2 = h2 * 5 + 0x38495ab5;
        }

        // Tail
        int tailIdx = offset + (nblocks * 16);
        long k1 = 0;
        long k2 = 0;

        switch (length & 15) {
            case 15: k2 ^= (long) (data[tailIdx + 14] & 0xff) << 48;
            case 14: k2 ^= (long) (data[tailIdx + 13] & 0xff) << 40;
            case 13: k2 ^= (long) (data[tailIdx + 12] & 0xff) << 32;
            case 12: k2 ^= (long) (data[tailIdx + 11] & 0xff) << 24;
            case 11: k2 ^= (long) (data[tailIdx + 10] & 0xff) << 16;
            case 10: k2 ^= (long) (data[tailIdx + 9] & 0xff) << 8;
            case 9:  k2 ^= (long) (data[tailIdx + 8] & 0xff);
                     k2 *= C2;
                     k2 = Long.rotateLeft(k2, 33);
                     k2 *= C1;
                     h2 ^= k2;

            case 8:  k1 ^= (long) (data[tailIdx + 7] & 0xff) << 56;
            case 7:  k1 ^= (long) (data[tailIdx + 6] & 0xff) << 48;
            case 6:  k1 ^= (long) (data[tailIdx + 5] & 0xff) << 40;
            case 5:  k1 ^= (long) (data[tailIdx + 4] & 0xff) << 32;
            case 4:  k1 ^= (long) (data[tailIdx + 3] & 0xff) << 24;
            case 3:  k1 ^= (long) (data[tailIdx + 2] & 0xff) << 16;
            case 2:  k1 ^= (long) (data[tailIdx + 1] & 0xff) << 8;
            case 1:  k1 ^= (long) (data[tailIdx] & 0xff);
                     k1 *= C1;
                     k1 = Long.rotateLeft(k1, 31);
                     k1 *= C2;
                     h1 ^= k1;
        }

        // Finalization
        h1 ^= length;
        h2 ^= length;

        h1 += h2;
        h2 += h1;

        h1 = fmix64(h1);
        h2 = fmix64(h2);

        h1 += h2;

        return h1;
    }

    /**
     * Generate a 64-bit hash from a string.
     *
     * @param key the string to hash
     * @return 64-bit hash value
     */
    public static long hash64(String key) {
        byte[] bytes = key.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        return hash64(bytes);
    }

    private static long fmix64(long k) {
        k ^= k >>> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;
        return k;
    }

    private static long getLongLittleEndian(byte[] data, int offset) {
        return ((long) data[offset] & 0xff)
             | (((long) data[offset + 1] & 0xff) << 8)
             | (((long) data[offset + 2] & 0xff) << 16)
             | (((long) data[offset + 3] & 0xff) << 24)
             | (((long) data[offset + 4] & 0xff) << 32)
             | (((long) data[offset + 5] & 0xff) << 40)
             | (((long) data[offset + 6] & 0xff) << 48)
             | (((long) data[offset + 7] & 0xff) << 56);
    }
}
