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
import java.net.Socket;
import java.net.SocketException;

/**
 * Wrapper around ServerSocket for Thrift.
 * 用于 Thrift 的封装的 ServerSocket
 */
public class TServerSocket extends TServerTransport {

    private static final Logger LOGGER = LoggerFactory.getLogger(TServerSocket.class.getName());

    /**
     * Underlying ServerSocket object
     * 底层的 ServerSocket 对象
     */
    private ServerSocket serverSocket_ = null;

    /**
     * Timeout for client sockets from accept
     * 客户端 Socket 接受超时时间
     */
    private int clientTimeout_ = 0;

    /**
     * Creates a server socket from underlying socket object
     * 使用底层的 Socket 对象构建 ServerSocket
     */
    public TServerSocket(ServerSocket serverSocket) throws TTransportException {
        this(serverSocket, 0);
    }

    /**
     * Creates a server socket from underlying socket object
     * 使用底层的 Socket 对象构建 ServerSocket
     */
    public TServerSocket(ServerSocket serverSocket, int clientTimeout) throws TTransportException {
        this(new ServerSocketTransportArgs().serverSocket(serverSocket)
                                            .clientTimeout(clientTimeout));
    }

    /**
     * Creates just a port listening server socket
     * 使用监听端口构建 ServerSocket
     */
    public TServerSocket(int port) throws TTransportException {
        this(port, 0);
    }

    /**
     * Creates just a port listening server socket
     * 使用监听端口构建 ServerSocket
     */
    public TServerSocket(int port, int clientTimeout) throws TTransportException {
        this(new InetSocketAddress(port), clientTimeout);
    }

    /**
     * 使用指定的地址构建 ServerSocket
     */
    public TServerSocket(InetSocketAddress bindAddr) throws TTransportException {
        this(bindAddr, 0);
    }

    /**
     * 使用地址和超时时间构建 ServerSocket
     *
     * @param bindAddr      监听地址
     * @param clientTimeout 超时时间
     */
    public TServerSocket(InetSocketAddress bindAddr, int clientTimeout) throws TTransportException {
        this(new ServerSocketTransportArgs().bindAddr(bindAddr)
                                            .clientTimeout(clientTimeout));
    }

    /**
     * 构建 ServerSocket
     *
     * @param args 构建参数
     */
    public TServerSocket(ServerSocketTransportArgs args) throws TTransportException {
        clientTimeout_ = args.clientTimeout;
        // 如果指定了 ServerSocket，则使用指定的
        if (args.serverSocket != null) {
            this.serverSocket_ = args.serverSocket;
            return;
        }
        try {
            // 如果没有则创建一个
            // Make server socket
            serverSocket_ = new ServerSocket();
            // Prevent 2MSL delay problem on server restarts
            // 表示是否允许重用服务器所绑定的地址；
            // 当ServerSocket关闭时，如果网络上还有发送到这个serversocket上的数据，
            // 这个ServerSocket不会立即释放本地端口，而是等待一段时间，确保接收到了网络上发送过来的延迟数据，然后再释放端口
            serverSocket_.setReuseAddress(true);
            // Bind to listening port
            serverSocket_.bind(args.bindAddr, args.backlog);
        } catch (IOException ioe) {
            close();
            throw new TTransportException("Could not create ServerSocket on address " + args.bindAddr.toString() + ".", ioe);
        }
    }

    /**
     * 设置超时时间
     */
    public void listen() throws TTransportException {
        // Make sure to block on accept
        if (serverSocket_ != null) {
            try {
                // 设置等待客户连接的超时时间
                serverSocket_.setSoTimeout(0);
            } catch (SocketException sx) {
                LOGGER.error("Could not set socket timeout.", sx);
            }
        }
    }

    /**
     * 监听并接受连接
     *
     * @return 建立连接之后的 TSocket
     */
    protected TSocket acceptImpl() throws TTransportException {
        if (serverSocket_ == null) {
            throw new TTransportException(TTransportException.NOT_OPEN, "No underlying server socket.");
        }
        try {
            // 监听并接受连接，会阻塞直到建立连接
            Socket result = serverSocket_.accept();
            // 用 Socket 构建 TSocket
            TSocket result2 = new TSocket(result);
            result2.setTimeout(clientTimeout_);
            return result2;
        } catch (IOException iox) {
            throw new TTransportException(iox);
        }
    }

    /**
     * 关闭 ServerSocket
     */
    public void close() {
        if (serverSocket_ != null) {
            try {
                serverSocket_.close();
            } catch (IOException iox) {
                LOGGER.warn("Could not close server socket.", iox);
            }
            serverSocket_ = null;
        }
    }

    /**
     * 打断 accept() 或 listen()
     */
    public void interrupt() {
        // The thread-safeness of this is dubious, but Java documentation suggests
        // that it is safe to do this from a different thread context
        close();
    }

    /**
     * 返回 ServerSocket
     */
    public ServerSocket getServerSocket() {
        return serverSocket_;
    }

    /**
     * Server 端 Transport 构建参数
     */
    public static class ServerSocketTransportArgs extends AbstractServerTransportArgs<ServerSocketTransportArgs> {

        ServerSocket serverSocket;

        public ServerSocketTransportArgs serverSocket(ServerSocket serverSocket) {
            this.serverSocket = serverSocket;
            return this;
        }
    }
}
