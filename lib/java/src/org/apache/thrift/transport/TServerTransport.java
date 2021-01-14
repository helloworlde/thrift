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
import java.net.InetSocketAddress;

/**
 * Server transport. Object which provides client transports.
 * Server 端的 Transport，提供客户端传输对象
 */
public abstract class TServerTransport implements Closeable {

    public abstract void listen() throws TTransportException;

    public final TTransport accept() throws TTransportException {
        TTransport transport = acceptImpl();
        if (transport == null) {
            throw new TTransportException("accept() may not return NULL");
        }
        return transport;
    }

    public abstract void close();

    protected abstract TTransport acceptImpl() throws TTransportException;

    /**
     * Optional method implementation. This signals to the server transport
     * that it should break out of any accept() or listen() that it is currently
     * blocked on. This method, if implemented, MUST be thread safe, as it may
     * be called from a different thread context than the other TServerTransport
     * methods.
     * 可选的方法实现，表示 Server Transport 是否应当打断阻塞的 accept() 或 listen()，
     * 如果实现了这个方法，必须是线程安全的，因为可能会被其他的 TServerTransport 的线程
     * 上下文访问
     */
    public void interrupt() {
    }

    /**
     * Server 端 Transport 构建参数
     */
    public static abstract class AbstractServerTransportArgs<T extends AbstractServerTransportArgs<T>> {
        // 如果是 0 表示会使用默认值，当前是 50
        // A value of 0 means the default value will be used (currently set at 50)
        int backlog = 0;

        // 超时时间
        int clientTimeout = 0;

        // 绑定的地址
        InetSocketAddress bindAddr;

        public T backlog(int backlog) {
            this.backlog = backlog;
            return (T) this;
        }

        public T clientTimeout(int clientTimeout) {
            this.clientTimeout = clientTimeout;
            return (T) this;
        }

        public T port(int port) {
            this.bindAddr = new InetSocketAddress(port);
            return (T) this;
        }

        public T bindAddr(InetSocketAddress bindAddr) {
            this.bindAddr = bindAddr;
            return (T) this;
        }
    }

}
