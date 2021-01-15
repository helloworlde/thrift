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
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

/**
 * Transport for use with async client.
 * 用于异步客户端的 Transport
 */
public class TNonblockingSocket extends TNonblockingTransport {

    private static final Logger LOGGER = LoggerFactory.getLogger(TNonblockingSocket.class.getName());

    /**
     * Host and port if passed in, used for lazy non-blocking connect.
     * 传入的主机和端口，用于懒加载非阻塞连接
     */
    private final SocketAddress socketAddress_;

    private final SocketChannel socketChannel_;

    public TNonblockingSocket(String host, int port) throws IOException {
        this(host, port, 0);
    }

    /**
     * Create a new nonblocking socket transport that will be connected to host:port.
     * 创建一个连接到 host:port 的非阻塞 Socket Transport
     */
    public TNonblockingSocket(String host, int port, int timeout) throws IOException {
        this(SocketChannel.open(), timeout, new InetSocketAddress(host, port));
    }

    /**
     * Constructor that takes an already created socket.
     *
     * @param socketChannel Already created SocketChannel object
     * @throws IOException if there is an error setting up the streams
     */
    public TNonblockingSocket(SocketChannel socketChannel) throws IOException {
        this(socketChannel, 0, null);
        if (!socketChannel.isConnected()) throw new IOException("Socket must already be connected");
    }

    private TNonblockingSocket(SocketChannel socketChannel,
                               int timeout,
                               SocketAddress socketAddress) throws IOException {
        socketChannel_ = socketChannel;
        socketAddress_ = socketAddress;

        // make it a nonblocking channel
        // 配置为非阻塞
        socketChannel.configureBlocking(false);

        // set options
        Socket socket = socketChannel.socket();
        // 接收完成才关闭
        socket.setSoLinger(false, 0);
        // TCP 分组
        socket.setTcpNoDelay(true);
        // 保持连接
        socket.setKeepAlive(true);
        setTimeout(timeout);
    }

    /**
     * Register the new SocketChannel with our Selector, indicating
     * we'd like to be notified when it's ready for I/O.
     * 将指定的 Selector注册到  SocketChannel，在 I/O 就绪时会接收到通知
     *
     * @param selector 选择器
     * @return the selection key for this socket.
     * 用于这个 Socket 的选择的 key
     */
    public SelectionKey registerSelector(Selector selector, int interests) throws IOException {
        return socketChannel_.register(selector, interests);
    }

    /**
     * Sets the socket timeout, although this implementation never uses blocking operations so it is unused.
     * 设置 Socket 超时时间
     *
     * @param timeout Milliseconds timeout
     */
    public void setTimeout(int timeout) {
        try {
            socketChannel_.socket()
                          .setSoTimeout(timeout);
        } catch (SocketException sx) {
            LOGGER.warn("Could not set socket timeout.", sx);
        }
    }

    /**
     * Returns a reference to the underlying SocketChannel.
     */
    public SocketChannel getSocketChannel() {
        return socketChannel_;
    }

    /**
     * Checks whether the socket is connected.
     */
    public boolean isOpen() {
        // isConnected() does not return false after close(), but isOpen() does
        return socketChannel_.isOpen() && socketChannel_.isConnected();
    }

    /**
     * Do not call, the implementation provides its own lazy non-blocking connect.
     */
    public void open() throws TTransportException {
        throw new RuntimeException("open() is not implemented for TNonblockingSocket");
    }

    /**
     * Perform a nonblocking read into buffer.
     * 非阻塞读取内容到 Buffer 中
     */
    public int read(ByteBuffer buffer) throws IOException {
        return socketChannel_.read(buffer);
    }


    /**
     * Reads from the underlying input stream if not null.
     * 从底层的流中读取
     */
    public int read(byte[] buf, int off, int len) throws TTransportException {
        if ((socketChannel_.validOps() & SelectionKey.OP_READ) != SelectionKey.OP_READ) {
            throw new TTransportException(TTransportException.NOT_OPEN,
                    "Cannot read from write-only socket channel");
        }
        try {
            return socketChannel_.read(ByteBuffer.wrap(buf, off, len));
        } catch (IOException iox) {
            throw new TTransportException(TTransportException.UNKNOWN, iox);
        }
    }

    /**
     * Perform a nonblocking write of the data in buffer;
     * 将缓冲区中的数据写入到 SocketChannel 中
     */
    public int write(ByteBuffer buffer) throws IOException {
        return socketChannel_.write(buffer);
    }

    /**
     * Writes to the underlying output stream if not null.
     * 将数据直接写入底层的输出流
     */
    public void write(byte[] buf, int off, int len) throws TTransportException {
        if ((socketChannel_.validOps() & SelectionKey.OP_WRITE) != SelectionKey.OP_WRITE) {
            throw new TTransportException(TTransportException.NOT_OPEN, "Cannot write to write-only socket channel");
        }
        try {
            // 写入
            socketChannel_.write(ByteBuffer.wrap(buf, off, len));
        } catch (IOException iox) {
            throw new TTransportException(TTransportException.UNKNOWN, iox);
        }
    }

    /**
     * Noop.
     */
    public void flush() throws TTransportException {
        // Not supported by SocketChannel.
    }

    /**
     * Closes the socket.
     * 关闭 Socket
     */
    public void close() {
        try {
            socketChannel_.close();
        } catch (IOException iox) {
            LOGGER.warn("Could not close socket.", iox);
        }
    }

    /**
     * {@inheritDoc}
     * 开始连接
     */
    public boolean startConnect() throws IOException {
        return socketChannel_.connect(socketAddress_);
    }

    /**
     * {@inheritDoc}
     * 完成上下文
     */
    public boolean finishConnect() throws IOException {
        return socketChannel_.finishConnect();
    }

}
