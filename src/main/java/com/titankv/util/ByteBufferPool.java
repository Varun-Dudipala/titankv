package com.titankv.util;

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Pool of reusable ByteBuffers to minimize GC pressure.
 * Uses ThreadLocal for thread-local buffers and a shared pool for overflow.
 */
public final class ByteBufferPool {

    private static final int DEFAULT_BUFFER_SIZE = 64 * 1024; // 64KB
    private static final int MAX_POOL_SIZE = 64;

    private final int bufferSize;
    private final boolean direct;
    private final Queue<ByteBuffer> pool;
    private final ThreadLocal<ByteBuffer> threadLocalBuffer;

    /**
     * Create a pool with default settings (64KB direct buffers).
     */
    public ByteBufferPool() {
        this(DEFAULT_BUFFER_SIZE, true);
    }

    /**
     * Create a pool with custom buffer size.
     *
     * @param bufferSize size of each buffer in bytes
     * @param direct     true for direct buffers (off-heap), false for heap buffers
     */
    public ByteBufferPool(int bufferSize, boolean direct) {
        this.bufferSize = bufferSize;
        this.direct = direct;
        this.pool = new ConcurrentLinkedQueue<>();
        this.threadLocalBuffer = ThreadLocal.withInitial(this::createBuffer);
    }

    /**
     * Get a buffer from the thread-local cache.
     * The buffer is cleared before returning.
     *
     * @return a cleared ByteBuffer
     */
    public ByteBuffer acquire() {
        ByteBuffer buffer = threadLocalBuffer.get();
        buffer.clear();
        return buffer;
    }

    /**
     * Get a buffer from the pool (not thread-local).
     * Use this when you need multiple buffers in the same thread.
     *
     * @return a cleared ByteBuffer
     */
    public ByteBuffer acquireFromPool() {
        ByteBuffer buffer = pool.poll();
        if (buffer == null) {
            buffer = createBuffer();
        } else {
            buffer.clear();
        }
        return buffer;
    }

    /**
     * Return a buffer to the pool.
     * Only call this for buffers acquired with acquireFromPool().
     *
     * @param buffer the buffer to return
     */
    public void release(ByteBuffer buffer) {
        if (buffer == null) {
            return;
        }
        if (buffer.capacity() != bufferSize) {
            return; // Wrong size, don't pool it
        }
        if (pool.size() < MAX_POOL_SIZE) {
            buffer.clear();
            pool.offer(buffer);
        }
        // If pool is full, let GC handle it
    }

    /**
     * Get a buffer of a specific size (not pooled).
     *
     * @param size the required size
     * @return a new ByteBuffer
     */
    public ByteBuffer acquireExact(int size) {
        if (size <= bufferSize) {
            return acquire();
        }
        // Create a larger buffer for this request
        return direct ? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);
    }

    /**
     * Get the standard buffer size.
     *
     * @return buffer size in bytes
     */
    public int getBufferSize() {
        return bufferSize;
    }

    /**
     * Get the current pool size.
     *
     * @return number of buffers in the pool
     */
    public int getPoolSize() {
        return pool.size();
    }

    /**
     * Clear the pool.
     */
    public void clear() {
        pool.clear();
    }

    private ByteBuffer createBuffer() {
        return direct ? ByteBuffer.allocateDirect(bufferSize) : ByteBuffer.allocate(bufferSize);
    }

    /**
     * Shared instance for common use.
     */
    private static final ByteBufferPool SHARED = new ByteBufferPool();

    /**
     * Get the shared pool instance.
     *
     * @return the shared ByteBufferPool
     */
    public static ByteBufferPool shared() {
        return SHARED;
    }
}
