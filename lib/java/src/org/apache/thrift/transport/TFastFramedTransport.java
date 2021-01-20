/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.thrift.transport;

/**
 * This transport is wire compatible with {@link TFramedTransport}, but makes
 * use of reusable, expanding read and write buffers in order to avoid
 * allocating new byte[]s all the time. Since the buffers only expand, you
 * should probably only use this transport if your messages are not too variably
 * large, unless the persistent memory cost is not an issue.
 * <p>
 * 这个 Transport 线性兼容 TFramedTransport，但是复用并扩展了读写缓冲区，避免每次都创建新的 byte 数组，
 * 因为是重用的，所以消息不应该太大，除非有足够的内存
 * <p>
 * This implementation is NOT threadsafe.
 * 这个实现不是线程安全的
 */
public class TFastFramedTransport extends TTransport {

    /**
     * How big should the default read and write buffers be?
     */
    public static final int DEFAULT_BUF_CAPACITY = 1024;
    /**
     * How big is the largest allowable frame? Defaults to 16MB.
     */
    public static final int DEFAULT_MAX_LENGTH = 16384000;
    private final TTransport underlying;
    // 可扩展的写缓冲区
    private final AutoExpandingBufferWriteTransport writeBuffer;
    private final int initialBufferCapacity;
    private final byte[] i32buf = new byte[4];
    private final int maxLength;
    // 可扩展的读缓冲区
    private AutoExpandingBufferReadTransport readBuffer;

    /**
     * Create a new {@link TFastFramedTransport}. Use the defaults
     * for initial buffer size and max frame length.
     *
     * @param underlying Transport that real reads and writes will go through to.
     */
    public TFastFramedTransport(TTransport underlying) {
        this(underlying, DEFAULT_BUF_CAPACITY, DEFAULT_MAX_LENGTH);
    }

    /**
     * Create a new {@link TFastFramedTransport}. Use the specified
     * initial buffer capacity and the default max frame length.
     *
     * @param underlying            Transport that real reads and writes will go through to.
     * @param initialBufferCapacity The initial size of the read and write buffers.
     *                              In practice, it's not critical to set this unless you know in advance that
     *                              your messages are going to be very large.
     */
    public TFastFramedTransport(TTransport underlying, int initialBufferCapacity) {
        this(underlying, initialBufferCapacity, DEFAULT_MAX_LENGTH);
    }

    /**
     * @param underlying            Transport that real reads and writes will go through to.
     * @param initialBufferCapacity The initial size of the read and write buffers.
     *                              In practice, it's not critical to set this unless you know in advance that
     *                              your messages are going to be very large. (You can pass
     *                              TFramedTransportWithReusableBuffer.DEFAULT_BUF_CAPACITY if you're only
     *                              using this constructor because you want to set the maxLength.)
     * @param maxLength             The max frame size you are willing to read. You can use
     *                              this parameter to limit how much memory can be allocated.
     */
    public TFastFramedTransport(TTransport underlying, int initialBufferCapacity, int maxLength) {
        this.underlying = underlying;
        this.maxLength = maxLength;
        this.initialBufferCapacity = initialBufferCapacity;
        readBuffer = new AutoExpandingBufferReadTransport(initialBufferCapacity);
        writeBuffer = new AutoExpandingBufferWriteTransport(initialBufferCapacity, 4);
    }

    @Override
    public void close() {
        underlying.close();
    }

    @Override
    public boolean isOpen() {
        return underlying.isOpen();
    }

    @Override
    public void open() throws TTransportException {
        underlying.open();
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
        int got = readBuffer.read(buf, off, len);
        if (got > 0) {
            return got;
        }

        // Read another frame of data
        // 如果没有读取到，则读取另一个帧
        readFrame();

        return readBuffer.read(buf, off, len);
    }

    private void readFrame() throws TTransportException {
        underlying.readAll(i32buf, 0, 4);
        int size = TFramedTransport.decodeFrameSize(i32buf);

        if (size < 0) {
            close();
            throw new TTransportException(TTransportException.CORRUPTED_DATA, "Read a negative frame size (" + size + ")!");
        }

        if (size > maxLength) {
            close();
            throw new TTransportException(TTransportException.CORRUPTED_DATA,
                    "Frame size (" + size + ") larger than max length (" + maxLength + ")!");
        }

        // 读取指定长度到 Transport
        readBuffer.fill(underlying, size);
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
        writeBuffer.write(buf, off, len);
    }

    @Override
    public void consumeBuffer(int len) {
        readBuffer.consumeBuffer(len);
    }

    /**
     * Only clears the read buffer!
     */
    public void clear() {
        readBuffer = new AutoExpandingBufferReadTransport(initialBufferCapacity);
    }

    /**
     * 将缓冲区数据写入到 Transport
     *
     * @throws TTransportException
     */
    @Override
    public void flush() throws TTransportException {
        int payloadLength = writeBuffer.getLength() - 4;
        byte[] data = writeBuffer.getBuf().array();
        TFramedTransport.encodeFrameSize(payloadLength, data);

        underlying.write(data, 0, payloadLength + 4);
        writeBuffer.reset();
        underlying.flush();
    }

    @Override
    public byte[] getBuffer() {
        return readBuffer.getBuffer();
    }

    @Override
    public int getBufferPosition() {
        return readBuffer.getBufferPosition();
    }

    @Override
    public int getBytesRemainingInBuffer() {
        return readBuffer.getBytesRemainingInBuffer();
    }

    public static class Factory extends TTransportFactory {

        private final int initialCapacity;

        private final int maxLength;

        public Factory() {
            this(DEFAULT_BUF_CAPACITY, DEFAULT_MAX_LENGTH);
        }

        public Factory(int initialCapacity) {
            this(initialCapacity, DEFAULT_MAX_LENGTH);
        }

        public Factory(int initialCapacity, int maxLength) {
            this.initialCapacity = initialCapacity;
            this.maxLength = maxLength;
        }

        @Override
        public TTransport getTransport(TTransport trans) {
            return new TFastFramedTransport(trans,
                    initialCapacity,
                    maxLength);
        }
    }
}
