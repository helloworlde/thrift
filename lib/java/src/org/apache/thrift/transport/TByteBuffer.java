package org.apache.thrift.transport;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

/**
 * ByteBuffer-backed implementation of TTransport.
 * <p>
 * 基于 ByteBuffer 实现的 Transport
 */
public final class TByteBuffer extends TTransport {

    private final ByteBuffer byteBuffer;

    /**
     * Creates a new TByteBuffer wrapping a given NIO ByteBuffer.
     */
    public TByteBuffer(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    public void open() {
    }

    @Override
    public void close() {
    }

    /**
     * 读取
     *
     * @param buf Array to read into
     *            要读取的字节数组
     * @param off Index to start reading at
     *            开始读取的索引下标
     * @param len Maximum number of bytes to read
     *            最大读取的字节数量
     * @return
     * @throws TTransportException
     */
    @Override
    public int read(byte[] buf, int off, int len) throws TTransportException {
        final int n = Math.min(byteBuffer.remaining(), len);
        if (n > 0) {
            try {
                byteBuffer.get(buf, off, n);
            } catch (BufferUnderflowException e) {
                throw new TTransportException("Unexpected end of input buffer", e);
            }
        }
        return n;
    }

    /**
     * 写入
     *
     * @param buf The output data buffer
     *            要输出的数据缓冲区
     * @param off The offset to start writing from
     *            开始写入的下标
     * @param len The number of bytes to write
     *            要写入的数据长度
     * @throws TTransportException
     */
    @Override
    public void write(byte[] buf, int off, int len) throws TTransportException {
        try {
            byteBuffer.put(buf, off, len);
        } catch (BufferOverflowException e) {
            throw new TTransportException("Not enough room in output buffer", e);
        }
    }

    /**
     * Get the underlying NIO ByteBuffer.
     */
    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    /**
     * Convenience method to call clear() on the underlying NIO ByteBuffer.
     */
    public TByteBuffer clear() {
        byteBuffer.clear();
        return this;
    }

    /**
     * Convenience method to call flip() on the underlying NIO ByteBuffer.
     */
    public TByteBuffer flip() {
        byteBuffer.flip();
        return this;
    }

    /**
     * Convenience method to convert the underlying NIO ByteBuffer to a
     * plain old byte array.
     */
    public byte[] toByteArray() {
        final byte[] data = new byte[byteBuffer.remaining()];
        byteBuffer.slice().get(data);
        return data;
    }
}
