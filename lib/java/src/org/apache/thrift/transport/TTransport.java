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

import java.io.Closeable;

/**
 * Generic class that encapsulates the I/O layer. This is basically a thin
 * wrapper around the combined functionality of Java input/output streams.
 * 封装了 IO 层的泛型类，是 Java IO 流的简单封装
 */
public abstract class TTransport implements Closeable {

    /**
     * Queries whether the transport is open.
     * 查询 Transport 是否开启
     *
     * @return True if the transport is open.
     * 如果开启则返回 true
     */
    public abstract boolean isOpen();

    /**
     * Is there more data to be read?
     * 是否有更多数据需要读取
     *
     * @return True if the remote side is still alive and feeding us
     * 如果远程端点存活并且有流则返回 true
     */
    public boolean peek() {
        return isOpen();
    }

    /**
     * Opens the transport for reading/writing.
     * 开启 Transport 用于读写
     *
     * @throws TTransportException if the transport could not be opened
     */
    public abstract void open() throws TTransportException;

    /**
     * Closes the transport.
     * 关闭 Transport
     */
    public abstract void close();

    /**
     * Reads up to len bytes into buffer buf, starting at offset off.
     * 从指定下标开始读取指定数量的字节数
     *
     * @param buf Array to read into
     *            要读取的字节数组
     * @param off Index to start reading at
     *            开始读取的索引下标
     * @param len Maximum number of bytes to read
     *            最大读取的字节数量
     * @return The number of bytes actually read
     * 真正读取的字节数量
     * @throws TTransportException if there was an error reading data
     */
    public abstract int read(byte[] buf, int off, int len) throws TTransportException;

    /**
     * Guarantees that all of len bytes are actually read off the transport.
     * 读取指定长度的所有的字节
     *
     * @param buf Array to read into
     *            要读取的字节
     * @param off Index to start reading at
     *            开始读取的下标
     * @param len Maximum number of bytes to read
     *            要读取的长度
     * @return The number of bytes actually read, which must be equal to len
     * 真正读取的长度，必须要和 len 相同
     * @throws TTransportException if there was an error reading data
     */
    public int readAll(byte[] buf, int off, int len) throws TTransportException {
        int got = 0;
        int ret = 0;
        while (got < len) {
            ret = read(buf, off + got, len - got);
            if (ret <= 0) {
                throw new TTransportException(
                        "Cannot read. Remote side has closed. Tried to read "
                                + len
                                + " bytes, but only got "
                                + got
                                + " bytes. (This is often indicative of an internal error on the server side. Please check your server logs.)");
            }
            got += ret;
        }
        return got;
    }

    /**
     * Writes the buffer to the output
     * 将输出数据写入缓冲区
     *
     * @param buf The output data buffer
     *            要输出的数据缓冲区
     * @throws TTransportException if an error occurs writing data
     */
    public void write(byte[] buf) throws TTransportException {
        write(buf, 0, buf.length);
    }

    /**
     * Writes up to len bytes from the buffer.
     * 将指定长度的字节数写入缓冲区
     *
     * @param buf The output data buffer
     *            要输出的数据缓冲区
     * @param off The offset to start writing from
     *            开始写入的下标
     * @param len The number of bytes to write
     *            要写入的数据长度
     * @throws TTransportException if there was an error writing data
     */
    public abstract void write(byte[] buf, int off, int len) throws TTransportException;

    /**
     * Flush any pending data out of a transport buffer.
     * 清空输出缓冲区中所有等待传递的数据
     *
     * @throws TTransportException if there was an error writing out data.
     */
    public void flush() throws TTransportException {
    }

    /**
     * Access the protocol's underlying buffer directly. If this is not a
     * buffered transport, return null.
     * 直接访问协议的基础缓冲区，如果不是缓冲的 Transport，则返回 null
     *
     * @return protocol's Underlying buffer
     * 协议的基础缓冲区
     */
    public byte[] getBuffer() {
        return null;
    }

    /**
     * Return the index within the underlying buffer that specifies the next spot
     * that should be read from.
     * 返回基础缓冲区内的索引，该索引指定应从中读取的下一个下标
     *
     * @return index within the underlying buffer that specifies the next spot
     * that should be read from
     */
    public int getBufferPosition() {
        return 0;
    }

    /**
     * Get the number of bytes remaining in the underlying buffer. Returns -1 if
     * this is a non-buffered transport.
     * 获取基础缓冲区中剩余的字节数。如果这是非缓冲 Transport，则返回-1
     *
     * @return the number of bytes remaining in the underlying buffer. <br> Returns -1 if
     * this is a non-buffered transport.
     */
    public int getBytesRemainingInBuffer() {
        return -1;
    }

    /**
     * Consume len bytes from the underlying buffer.
     * 从基础缓冲区中读取指定长度的字节
     *
     * @param len 要读取的数据长度
     */
    public void consumeBuffer(int len) {
    }
}
