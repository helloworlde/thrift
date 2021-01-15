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
import java.net.ServerSocket;
import java.net.SocketException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

/**
 * Wrapper around ServerSocketChannel
 * 封装的 ServerSocketChannel
 */
public class TNonblockingServerSocket extends TNonblockingServerTransport {

    private static final Logger LOGGER = LoggerFactory.getLogger(TNonblockingServerSocket.class.getName());

    /**
     * This channel is where all the nonblocking magic happens.
     * <p>
     * 非阻塞的 Channel
     */
    private ServerSocketChannel serverSocketChannel = null;

    /**
     * Underlying ServerSocket object
     * 底层的 ServerSocket 对象
     */
    private ServerSocket serverSocket_ = null;

    /**
     * Timeout for client sockets from accept
     * 客户端 Socket 连接超时时间
     */
    private int clientTimeout_ = 0;

    /**
     * Creates just a port listening server socket
     * 使用 Port 创建监听的 ServerSocket
     */
    public TNonblockingServerSocket(int port) throws TTransportException {
        this(port, 0);
    }

    /**
     * Creates just a port listening server socket
     * 使用 Port 和超时时间创建监听的 ServerSocket
     */
    public TNonblockingServerSocket(int port, int clientTimeout) throws TTransportException {
        this(new NonblockingAbstractServerSocketArgs().port(port)
                                                      .clientTimeout(clientTimeout));
    }

    public TNonblockingServerSocket(InetSocketAddress bindAddr) throws TTransportException {
        this(bindAddr, 0);
    }

    public TNonblockingServerSocket(InetSocketAddress bindAddr,
                                    int clientTimeout) throws TTransportException {
        this(new NonblockingAbstractServerSocketArgs().bindAddr(bindAddr)
                                                      .clientTimeout(clientTimeout));
    }

    public TNonblockingServerSocket(NonblockingAbstractServerSocketArgs args) throws TTransportException {
        clientTimeout_ = args.clientTimeout;
        try {
            // 开启 ServerSocket
            serverSocketChannel = ServerSocketChannel.open();
            // 非阻塞
            serverSocketChannel.configureBlocking(false);

            // Make server socket
            // 获取 Socket
            serverSocket_ = serverSocketChannel.socket();
            // Prevent 2MSL delay problem on server restarts
            // 重用 IP 地址，等待消息接收完成之后关闭
            serverSocket_.setReuseAddress(true);
            // Bind to listening port
            // 绑定监听的地址
            serverSocket_.bind(args.bindAddr, args.backlog);
        } catch (IOException ioe) {
            serverSocket_ = null;
            throw new TTransportException("Could not create ServerSocket on address " + args.bindAddr.toString() + ".", ioe);
        }
    }

    public void listen() throws TTransportException {
        // Make sure not to block on accept
        if (serverSocket_ != null) {
            try {
                serverSocket_.setSoTimeout(0);
            } catch (SocketException sx) {
                LOGGER.error("Socket exception while setting socket timeout", sx);
            }
        }
    }

    /**
     * 接受客户端连接
     */
    protected TNonblockingSocket acceptImpl() throws TTransportException {
        if (serverSocket_ == null) {
            throw new TTransportException(TTransportException.NOT_OPEN, "No underlying server socket.");
        }
        try {
            // 接受连接
            SocketChannel socketChannel = serverSocketChannel.accept();
            if (socketChannel == null) {
                return null;
            }

            // 使用 Channel 构建 Socket
            TNonblockingSocket tsocket = new TNonblockingSocket(socketChannel);
            tsocket.setTimeout(clientTimeout_);
            return tsocket;
        } catch (IOException iox) {
            throw new TTransportException(iox);
        }
    }

    /**
     * 注册选择器
     */
    public void registerSelector(Selector selector) {
        try {
            // Register the server socket channel, indicating an interest in
            // accepting new connections
            // 注册服务器套接字通道，表示有兴趣接受新连接
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        } catch (ClosedChannelException e) {
            // this shouldn't happen, ideally...
            // TODO: decide what to do with this.
        }
    }

    /**
     * 关闭连接
     */
    public void close() {
        if (serverSocket_ != null) {
            try {
                serverSocket_.close();
            } catch (IOException iox) {
                LOGGER.warn("WARNING: Could not close server socket: " + iox.getMessage());
            }
            serverSocket_ = null;
        }
    }

    public void interrupt() {
        // The thread-safeness of this is dubious, but Java documentation suggests
        // that it is safe to do this from a different thread context
        close();
    }

    public int getPort() {
        if (serverSocket_ == null) {
            return -1;
        }
        return serverSocket_.getLocalPort();
    }

    /**
     * 非阻塞 ServerSocket 参数
     */
    public static class NonblockingAbstractServerSocketArgs extends AbstractServerTransportArgs<NonblockingAbstractServerSocketArgs> {
    }

}
