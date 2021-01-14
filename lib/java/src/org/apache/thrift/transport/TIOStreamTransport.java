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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * This is the most commonly used base transport. It takes an InputStream
 * and an OutputStream and uses those to perform all transport operations.
 * This allows for compatibility with all the nice constructs Java already
 * has to provide a variety of types of streams.
 * <p>
 * 最常使用的基础的 Transport，使用一个 InputStream 和 OutputStream，进行所有的
 * Transport 操作；与 Java 已经提供的各种类型的流兼容
 */

public class TIOStreamTransport extends TTransport {

    private static final Logger LOGGER = LoggerFactory.getLogger(TIOStreamTransport.class.getName());

    /**
     * Underlying inputStream
     * 底层的 InputStream
     */
    protected InputStream inputStream_ = null;

    /**
     * Underlying outputStream
     * 底层的 OutputStream
     */
    protected OutputStream outputStream_ = null;

    /**
     * Subclasses can invoke the default constructor and then assign the input
     * streams in the open method.
     * 子类可以调用默认的构造器，可以在 open 方法中设置输入流
     */
    protected TIOStreamTransport() {
    }

    /**
     * Input stream constructor.
     * 使用输入流构造
     *
     * @param is Input stream to read from
     *           要读取的输入流
     */
    public TIOStreamTransport(InputStream is) {
        inputStream_ = is;
    }

    /**
     * Output stream constructor.
     * 使用输出流构造
     *
     * @param os Output stream to read from
     *           要读取的输出流
     */
    public TIOStreamTransport(OutputStream os) {
        outputStream_ = os;
    }

    /**
     * Two-way stream constructor.
     * 双向流构造器
     *
     * @param is Input stream to read from
     *           读取的输入流
     * @param os Output stream to read from
     *           读取的输出流
     */
    public TIOStreamTransport(InputStream is, OutputStream os) {
        inputStream_ = is;
        outputStream_ = os;
    }

    /**
     * @return false after close is called.
     * 当调用了 close 之后返回 false
     */
    public boolean isOpen() {
        return inputStream_ != null && outputStream_ != null;
    }

    /**
     * The streams must already be open. This method does nothing.
     * 流必须已经开启，这个方法不做任何操作
     */
    public void open() throws TTransportException {
    }

    /**
     * Closes both the input and output streams.
     * 关闭输入和输出流
     */
    public void close() {
        // 关闭输入流
        if (inputStream_ != null) {
            try {
                inputStream_.close();
            } catch (IOException iox) {
                LOGGER.warn("Error closing input stream.", iox);
            }
            inputStream_ = null;
        }
        // 关闭输出流
        if (outputStream_ != null) {
            try {
                outputStream_.close();
            } catch (IOException iox) {
                LOGGER.warn("Error closing output stream.", iox);
            }
            outputStream_ = null;
        }
    }

    /**
     * Reads from the underlying input stream if not null.
     * 如果不是null，则从底层输入流读取
     */
    public int read(byte[] buf, int off, int len) throws TTransportException {
        if (inputStream_ == null) {
            throw new TTransportException(TTransportException.NOT_OPEN, "Cannot read from null inputStream");
        }
        int bytesRead;
        try {
            // 读取指定的字节数
            bytesRead = inputStream_.read(buf, off, len);
        } catch (IOException iox) {
            throw new TTransportException(TTransportException.UNKNOWN, iox);
        }
        if (bytesRead < 0) {
            throw new TTransportException(TTransportException.END_OF_FILE, "Socket is closed by peer.");
        }
        return bytesRead;
    }

    /**
     * Writes to the underlying output stream if not null.
     * 写入指定的数据到输出流
     */
    public void write(byte[] buf, int off, int len) throws TTransportException {
        if (outputStream_ == null) {
            throw new TTransportException(TTransportException.NOT_OPEN, "Cannot write to null outputStream");
        }
        try {
            outputStream_.write(buf, off, len);
        } catch (IOException iox) {
            throw new TTransportException(TTransportException.UNKNOWN, iox);
        }
    }

    /**
     * Flushes the underlying output stream if not null.
     * 清空底层输出流
     */
    public void flush() throws TTransportException {
        if (outputStream_ == null) {
            throw new TTransportException(TTransportException.NOT_OPEN, "Cannot flush null outputStream");
        }
        try {
            outputStream_.flush();
        } catch (IOException iox) {
            throw new TTransportException(TTransportException.UNKNOWN, iox);
        }
    }
}
