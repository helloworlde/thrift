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
 * TTransport for writing to an AutoExpandingBuffer.
 * 用于写入 AutoExpandingBuffer 的 Transport
 */
public final class AutoExpandingBufferWriteTransport extends TTransport {

    // 可扩展的缓冲区
    private final AutoExpandingBuffer buf;

    private int pos;

    private int res;

    /**
     * Constructor.
     *
     * @param initialCapacity the initial capacity of the buffer
     * @param frontReserve    space, if any, to reserve at the beginning such
     *                        that the first write is after this reserve.
     *                        This allows framed transport to reserve space
     *                        for the frame buffer length.
     *                        如果有的话，要在开始时保留空间，以便在此保留之后进行第一次写操作，
     *                        这允许成帧传输为帧缓冲区长度保留空间
     * @throws IllegalArgumentException if initialCapacity is less than one
     * @throws IllegalArgumentException if frontReserve is less than zero
     * @throws IllegalArgumentException if frontReserve is greater than initialCapacity
     */
    public AutoExpandingBufferWriteTransport(int initialCapacity, int frontReserve) {
        if (initialCapacity < 1) {
            throw new IllegalArgumentException("initialCapacity");
        }
        if (frontReserve < 0 || initialCapacity < frontReserve) {
            throw new IllegalArgumentException("frontReserve");
        }
        this.buf = new AutoExpandingBuffer(initialCapacity);
        this.pos = frontReserve;
        this.res = frontReserve;
    }

    @Override
    public void close() {
    }

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    public void open() throws TTransportException {
    }

    @Override
    public int read(byte[] buf, int off, int len) throws TTransportException {
        throw new UnsupportedOperationException();
    }

    /**
     * 写入
     *
     * @param toWrite 要写入的数组
     * @param off     The offset to start writing from
     *                开始写入的下标
     * @param len     The number of bytes to write
     *                要写入的数据长度
     * @throws TTransportException
     */
    @Override
    public void write(byte[] toWrite, int off, int len) throws TTransportException {
        buf.resizeIfNecessary(pos + len);
        System.arraycopy(toWrite, off, buf.array(), pos, len);
        pos += len;
    }

    public AutoExpandingBuffer getBuf() {
        return buf;
    }

    /**
     * @return length of the buffer, including any front reserve
     */
    public int getLength() {
        return pos;
    }

    public void reset() {
        pos = res;
    }
}
