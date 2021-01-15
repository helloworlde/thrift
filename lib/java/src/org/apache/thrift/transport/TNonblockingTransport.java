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

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;

/**
 * 非阻塞的 Transport
 */
public abstract class TNonblockingTransport extends TTransport {

    /**
     * Non-blocking connection initialization.
     * 初始化非阻塞的连接
     *
     * @see java.nio.channels.SocketChannel#connect(SocketAddress remote)
     */
    public abstract boolean startConnect() throws IOException;

    /**
     * Non-blocking connection completion.
     * 非阻塞连接完成
     *
     * @see java.nio.channels.SocketChannel#finishConnect()
     */
    public abstract boolean finishConnect() throws IOException;

    /**
     * 注册选择器
     */
    public abstract SelectionKey registerSelector(Selector selector, int interests) throws IOException;

    /**
     * 读取内容到缓冲区
     */
    public abstract int read(ByteBuffer buffer) throws IOException;

    /**
     * 将缓冲区数据输出到连接
     */
    public abstract int write(ByteBuffer buffer) throws IOException;
}
