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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;

/**
 * Socket implementation of the TTransport interface. To be commented soon!
 * Transport 接口的 Socket 实现
 */
public class TSocket extends TIOStreamTransport {

    private static final Logger LOGGER = LoggerFactory.getLogger(TSocket.class.getName());

    /**
     * Wrapped Socket object
     * 被封装的 Socket
     */
    private Socket socket_;

    /**
     * Remote host
     * 远程的地址
     */
    private String host_;

    /**
     * Remote port
     * 远程的端口
     */
    private int port_;

    /**
     * Socket timeout - read timeout on the socket
     * Socket 读取超时时间
     */
    private int socketTimeout_;

    /**
     * Connection timeout
     * 连接超时时间
     */
    private int connectTimeout_;

    /**
     * Constructor that takes an already created socket.
     * 使用已经创建的 Socket 构建 TSocket
     *
     * @param socket Already created socket object
     * @throws TTransportException if there is an error setting up the streams
     */
    public TSocket(Socket socket) throws TTransportException {
        socket_ = socket;
        try {
            // SO_LINGER 选项用来控制 Socket 关闭时的行为，默认情况下，
            // 执行 Socket 的 close 方法，该方法会立即返回，但底层的 Socket 实际上并不会立即关闭，
            // 会立即延迟一段时间，直到发送完剩余的数据，才会真正的关闭 Socket，断开连接
            socket_.setSoLinger(false, 0);
            // TCP_NODELAY 选项是用来控制是否开启 Nagle 算法，
            // 该算法是为了提高较慢的广域网传输效率，减小小分组的报文个数
            // 该算法要求一个 TCP 连接上最多只能有一个未被确认的小分组，在该小分组的确认到来之前，不能发送其他小分组
            socket_.setTcpNoDelay(true);
            // 可以通过应用层实现了解服务端或客户端状态，而决定是否继续维持该 Socket
            // 当对方没有发送任何数据过来，超过特定时间，会发送一个 ack 探测包发到对方，探测双方的 TCP/IP 连接是否有效
            socket_.setKeepAlive(true);
        } catch (SocketException sx) {
            LOGGER.warn("Could not configure socket.", sx);
        }

        // 如果 Socket 是连接的，则使用 Socket 的流构建并赋值
        if (isOpen()) {
            try {
                inputStream_ = new BufferedInputStream(socket_.getInputStream());
                outputStream_ = new BufferedOutputStream(socket_.getOutputStream());
            } catch (IOException iox) {
                close();
                throw new TTransportException(TTransportException.NOT_OPEN, iox);
            }
        }
    }

    /**
     * Creates a new unconnected socket that will connect to the given host
     * on the given port.
     * 使用给定的主机和端口构建一个未连接的 Socket
     *
     * @param host Remote host
     *             远程的主机
     * @param port Remote port
     *             远程的端口
     */
    public TSocket(String host, int port) {
        this(host, port, 0);
    }

    /**
     * Creates a new unconnected socket that will connect to the given host
     * on the given port.
     * 使用给定的主机、端口和超时时间构建一个未连接的 Socket
     *
     * @param host    Remote host
     *                远程的主机
     * @param port    Remote port
     *                远程的端口
     * @param timeout Socket timeout and connection timeout
     *                socket 读取超时时间和连接超时时间
     */
    public TSocket(String host, int port, int timeout) {
        this(host, port, timeout, timeout);
    }

    /**
     * Creates a new unconnected socket that will connect to the given host
     * on the given port, with a specific connection timeout and a
     * specific socket timeout.
     * 使用给定的主机、端口、socket 读取超时时间、连接超时时间构建一个未连接的 Socket
     *
     * @param host           Remote host
     *                       远程的主机
     * @param port           Remote port
     *                       远程的端口
     * @param socketTimeout  Socket timeout
     *                       Socket 读取超时时间
     * @param connectTimeout Connection timeout
     *                       连接超时时间
     */
    public TSocket(String host, int port, int socketTimeout, int connectTimeout) {
        host_ = host;
        port_ = port;
        socketTimeout_ = socketTimeout;
        connectTimeout_ = connectTimeout;
        initSocket();
    }

    /**
     * Initializes the socket object
     * 初始化 Socket 对象
     */
    private void initSocket() {
        // 创建 Socket
        socket_ = new Socket();
        try {
            // Socket 关闭行为
            socket_.setSoLinger(false, 0);
            // 开启 Nagle
            socket_.setTcpNoDelay(true);
            // 保持连接
            socket_.setKeepAlive(true);
            // Socket 读取超时时间
            socket_.setSoTimeout(socketTimeout_);
        } catch (SocketException sx) {
            LOGGER.error("Could not configure socket.", sx);
        }
    }

    /**
     * Sets the socket timeout and connection timeout.
     * 设置 Socket 读取超时时间和连接超时时间
     *
     * @param timeout Milliseconds timeout
     *                超时时间
     */
    public void setTimeout(int timeout) {
        this.setConnectTimeout(timeout);
        this.setSocketTimeout(timeout);
    }

    /**
     * Sets the time after which the connection attempt will time out
     *
     * @param timeout Milliseconds timeout
     */
    public void setConnectTimeout(int timeout) {
        connectTimeout_ = timeout;
    }

    /**
     * Sets the socket timeout
     *
     * @param timeout Milliseconds timeout
     */
    public void setSocketTimeout(int timeout) {
        socketTimeout_ = timeout;
        try {
            socket_.setSoTimeout(timeout);
        } catch (SocketException sx) {
            LOGGER.warn("Could not set socket timeout.", sx);
        }
    }

    /**
     * Returns a reference to the underlying socket.
     * 返回底层的 Socket
     */
    public Socket getSocket() {
        if (socket_ == null) {
            initSocket();
        }
        return socket_;
    }

    /**
     * Checks whether the socket is connected.
     * 检查 Socket 是否连接
     */
    public boolean isOpen() {
        if (socket_ == null) {
            return false;
        }
        return socket_.isConnected();
    }

    /**
     * Connects the socket, creating a new socket object if necessary.
     * 连接 Socket，必要时创建新的 Socket 对象
     */
    public void open() throws TTransportException {
        if (isOpen()) {
            throw new TTransportException(TTransportException.ALREADY_OPEN, "Socket already connected.");
        }

        if (host_ == null || host_.length() == 0) {
            throw new TTransportException(TTransportException.NOT_OPEN, "Cannot open null host.");
        }
        if (port_ <= 0 || port_ > 65535) {
            throw new TTransportException(TTransportException.NOT_OPEN, "Invalid port " + port_);
        }

        if (socket_ == null) {
            // 初始化 Socket
            initSocket();
        }

        try {
            // 建立连接
            socket_.connect(new InetSocketAddress(host_, port_), connectTimeout_);
            // 初始化流
            inputStream_ = new BufferedInputStream(socket_.getInputStream());
            outputStream_ = new BufferedOutputStream(socket_.getOutputStream());
        } catch (IOException iox) {
            close();
            throw new TTransportException(TTransportException.NOT_OPEN, iox);
        }
    }

    /**
     * Closes the socket.
     * 关闭 Socket
     */
    public void close() {
        // Close the underlying streams
        super.close();

        // Close the socket
        if (socket_ != null) {
            try {
                socket_.close();
            } catch (IOException iox) {
                LOGGER.warn("Could not close socket.", iox);
            }
            socket_ = null;
        }
    }

}
