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

import org.apache.thrift.TByteArrayOutputStream;

/**
 * TFramedTransport is a buffered TTransport that ensures a fully read message
 * every time by preceding messages with a 4-byte frame size.
 * <p>
 * TFramedTransport 是一个缓冲的 Transport，通过在前面带有4字节帧大小的消息来确保每次都完全读取消息
 */
public class TFramedTransport extends TTransport {

    protected static final int DEFAULT_MAX_LENGTH = 16384000;
    /**
     * Something to fill in the first four bytes of the buffer
     * to make room for the frame size.  This allows the
     * implementation to write once instead of twice.
     * <p>
     * 填充帧开始的四个字节，允许实现一次写入而不用写入两次
     */
    private static final byte[] sizeFiller_ = new byte[]{0x00, 0x00, 0x00, 0x00};
    /**
     * Buffer for output
     * 用于输出的缓冲区
     */
    private final TByteArrayOutputStream writeBuffer_ = new TByteArrayOutputStream(1024);
    /**
     * Buffer for input
     * 用于输入的缓冲区
     */
    private final TMemoryInputTransport readBuffer_ = new TMemoryInputTransport(new byte[0]);
    private final byte[] i32buf = new byte[4];
    private int maxLength_;
    /**
     * Underlying transport
     * 底层的 Transport
     */
    private TTransport transport_ = null;

    /**
     * Constructor wraps around another transport
     * <p>
     * 封装另一个 Transport
     */
    public TFramedTransport(TTransport transport, int maxLength) {
        transport_ = transport;
        maxLength_ = maxLength;
        writeBuffer_.write(sizeFiller_, 0, 4);
    }

    public TFramedTransport(TTransport transport) {
        transport_ = transport;
        maxLength_ = TFramedTransport.DEFAULT_MAX_LENGTH;
        writeBuffer_.write(sizeFiller_, 0, 4);
    }

    public static final void encodeFrameSize(final int frameSize, final byte[] buf) {
        buf[0] = (byte) (0xff & (frameSize >> 24));
        buf[1] = (byte) (0xff & (frameSize >> 16));
        buf[2] = (byte) (0xff & (frameSize >> 8));
        buf[3] = (byte) (0xff & (frameSize));
    }

    public static final int decodeFrameSize(final byte[] buf) {
        return ((buf[0] & 0xff) << 24) |
                ((buf[1] & 0xff) << 16) |
                ((buf[2] & 0xff) << 8) |
                ((buf[3] & 0xff));
    }

    public void open() throws TTransportException {
        transport_.open();
    }

    public boolean isOpen() {
        return transport_.isOpen();
    }

    public void close() {
        transport_.close();
    }

    /**
     * 从指定下标开始读取指定数量的字节数
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
    public int read(byte[] buf, int off, int len) throws TTransportException {
        int got = readBuffer_.read(buf, off, len);
        if (got > 0) {
            return got;
        }

        // Read another frame of data
        readFrame();

        return readBuffer_.read(buf, off, len);
    }

    @Override
    public byte[] getBuffer() {
        return readBuffer_.getBuffer();
    }

    @Override
    public int getBufferPosition() {
        return readBuffer_.getBufferPosition();
    }

    @Override
    public int getBytesRemainingInBuffer() {
        return readBuffer_.getBytesRemainingInBuffer();
    }

    @Override
    public void consumeBuffer(int len) {
        readBuffer_.consumeBuffer(len);
    }

    public void clear() {
        readBuffer_.clear();
    }

    /**
     * 读取帧
     */
    private void readFrame() throws TTransportException {
        // 读取前四个字节，获取消息大小
        transport_.readAll(i32buf, 0, 4);
        int size = decodeFrameSize(i32buf);

        if (size < 0) {
            close();
            throw new TTransportException(TTransportException.CORRUPTED_DATA, "Read a negative frame size (" + size + ")!");
        }

        if (size > maxLength_) {
            close();
            throw new TTransportException(TTransportException.CORRUPTED_DATA,
                    "Frame size (" + size + ") larger than max length (" + maxLength_ + ")!");
        }

        // 构建响应大小的字节 完全读取
        byte[] buff = new byte[size];
        transport_.readAll(buff, 0, size);
        readBuffer_.reset(buff);
    }

    /**
     * 写入消息
     *
     * @param buf The output data buffer
     *            要输出的数据缓冲区
     * @param off The offset to start writing from
     *            开始写入的下标
     * @param len The number of bytes to write
     *            要写入的数据长度
     * @throws TTransportException
     */
    public void write(byte[] buf, int off, int len) throws TTransportException {
        writeBuffer_.write(buf, off, len);
    }

    /**
     * 清空缓冲区
     */
    @Override
    public void flush() throws TTransportException {
        byte[] buf = writeBuffer_.get();
        int len = writeBuffer_.len() - 4;       // account for the prepended frame size
        writeBuffer_.reset();
        writeBuffer_.write(sizeFiller_, 0, 4);  // make room for the next frame's size data

        encodeFrameSize(len, buf);              // this is the frame length without the filler
        transport_.write(buf, 0, len + 4);      // we have to write the frame size and frame data
        transport_.flush();
    }

    public static class Factory extends TTransportFactory {
        private int maxLength_;

        public Factory() {
            maxLength_ = TFramedTransport.DEFAULT_MAX_LENGTH;
        }

        public Factory(int maxLength) {
            maxLength_ = maxLength;
        }

        @Override
        public TTransport getTransport(TTransport base) {
            return new TFramedTransport(base, maxLength_);
        }
    }
}
