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


package org.apache.thrift.server;

import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.Iterator;

/**
 * A nonblocking TServer implementation. This allows for fairness amongst all
 * connected clients in terms of invocations.
 * 非阻塞的 Server 实现，允许所有连接的客户端之间在调用方面保持公平
 * <p>
 * This server is inherently single-threaded. If you want a limited thread pool
 * coupled with invocation-fairness, see THsHaServer.
 * 服务器是单线程的，如果想使用线程池保持公平，可以用 THsHaServer
 * <p>
 * To use this server, you MUST use a TFramedTransport at the outermost
 * transport, otherwise this server will be unable to determine when a whole
 * method call has been read off the wire. Clients must also use TFramedTransport.
 * 这个 Server 必须使用 TFramedTransport 作为最外层的 Transport，否则
 * 该服务器将无法确定何时从网络上读取了整个方法调用，客户端也必须使用 TFramedTransport
 */
public class TNonblockingServer extends AbstractNonblockingServer {

    private SelectAcceptThread selectAcceptThread_;

    public TNonblockingServer(AbstractNonblockingServerArgs args) {
        super(args);
    }

    /**
     * Start the selector thread to deal with accepts and client messages.
     * 启动 Selector 线程，允许与客户端通信
     *
     * @return true if everything went ok, false if we couldn't start for some
     * reason. 当一切正常的时候返回 true，如果无法开始则返回 false
     */
    @Override
    protected boolean startThreads() {
        // start the selector
        try {
            selectAcceptThread_ = new SelectAcceptThread((TNonblockingServerTransport) serverTransport_);
            selectAcceptThread_.start();
            return true;
        } catch (IOException e) {
            LOGGER.error("Failed to start selector thread!", e);
            return false;
        }
    }

    @Override
    protected void waitForShutdown() {
        joinSelector();
    }

    /**
     * Block until the selector thread exits.
     * 阻塞直到 Selector 线程退出
     */
    protected void joinSelector() {
        // wait until the selector thread exits
        try {
            selectAcceptThread_.join();
        } catch (InterruptedException e) {
            LOGGER.debug("Interrupted while waiting for accept thread", e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Stop serving and shut everything down.
     * 关闭 Server
     */
    @Override
    public void stop() {
        stopped_ = true;
        if (selectAcceptThread_ != null) {
            selectAcceptThread_.wakeupSelector();
        }
    }

    /**
     * Perform an invocation. This method could behave several different ways
     * - invoke immediately inline, queue for separate execution, etc.
     * 执行调用
     */
    @Override
    protected boolean requestInvoke(FrameBuffer frameBuffer) {
        frameBuffer.invoke();
        return true;
    }

    public boolean isStopped() {
        return selectAcceptThread_.isStopped();
    }

    public static class Args extends AbstractNonblockingServerArgs<Args> {
        public Args(TNonblockingServerTransport transport) {
            super(transport);
        }
    }

    /**
     * The thread that will be doing all the selecting, managing new connections
     * and those that still need to be read.
     * 这个线程负责所有的 select，包括新的连接和需要读取的连接
     */
    protected class SelectAcceptThread extends AbstractSelectThread {

        // The server transport on which new client transports will be accepted
        private final TNonblockingServerTransport serverTransport;

        /**
         * Set up the thread that will handle the non-blocking accepts, reads, and
         * writes.
         * 用于处理非阻塞连接，读取和写入的线程
         */
        public SelectAcceptThread(final TNonblockingServerTransport serverTransport) throws IOException {
            this.serverTransport = serverTransport;
            serverTransport.registerSelector(selector);
        }

        public boolean isStopped() {
            return stopped_;
        }

        /**
         * The work loop. Handles both selecting (all IO operations) and managing
         * the selection preferences of all existing connections.
         * 工作线程循环，处理所有的 IO 操作，管理存在的连接
         */
        public void run() {
            try {
                // Server 开始对外工作
                if (eventHandler_ != null) {
                    eventHandler_.preServe();
                }
                // 只要没有停止，就执行 select 和处理连接变化
                while (!stopped_) {
                    select();
                    processInterestChanges();
                }
                for (SelectionKey selectionKey : selector.keys()) {
                    cleanupSelectionKey(selectionKey);
                }
            } catch (Throwable t) {
                LOGGER.error("run() exiting due to uncaught error", t);
            } finally {
                try {
                    selector.close();
                } catch (IOException e) {
                    LOGGER.error("Got an IOException while closing selector!", e);
                }
                stopped_ = true;
            }
        }

        /**
         * Select and process IO events appropriately:
         * If there are connections to be accepted, accept them.
         * If there are existing connections with data waiting to be read, read it,
         * buffering until a whole frame has been read.
         * If there are any pending responses, buffer them until their target client
         * is available, and then send the data.
         * <p>
         * 选择并适当处理 IO 事件：
         * 如果连接需要建立，则建立
         * 如果连接中有数据需要读取，则读取数据，直到整个帧被读取完
         * 如果有等待相应的，缓冲直到客户端可用，并发送数据
         */
        private void select() {
            try {
                // wait for io events.
                // 等待 IO 事件
                selector.select();

                // process the io events we received
                // 处理接收到的 IO 事件
                Iterator<SelectionKey> selectedKeys = selector.selectedKeys().iterator();
                while (!stopped_ && selectedKeys.hasNext()) {
                    SelectionKey key = selectedKeys.next();
                    selectedKeys.remove();

                    // skip if not valid
                    // 如果无效则清理
                    if (!key.isValid()) {
                        cleanupSelectionKey(key);
                        continue;
                    }

                    // if the key is marked Accept, then it has to be the server
                    // transport.
                    // 如果是连接，则建立新的连接
                    if (key.isAcceptable()) {
                        handleAccept();
                    } else if (key.isReadable()) {
                        // 处理读取
                        // deal with reads
                        handleRead(key);
                    } else if (key.isWritable()) {
                        // deal with writes
                        // 处理写入
                        handleWrite(key);
                    } else {
                        LOGGER.warn("Unexpected state in select! " + key.interestOps());
                    }
                }
            } catch (IOException e) {
                LOGGER.warn("Got an IOException while selecting!", e);
            }
        }

        /**
         * 创建帧缓冲
         */
        protected FrameBuffer createFrameBuffer(final TNonblockingTransport trans,
                                                final SelectionKey selectionKey,
                                                final AbstractSelectThread selectThread) {
            return processorFactory_.isAsyncProcessor() ?
                    new AsyncFrameBuffer(trans, selectionKey, selectThread) :
                    new FrameBuffer(trans, selectionKey, selectThread);
        }

        /**
         * Accept a new connection.
         * 建立连接
         */
        private void handleAccept() throws IOException {
            SelectionKey clientKey = null;
            TNonblockingTransport client = null;
            try {
                // accept the connection
                // 接受连接
                client = (TNonblockingTransport) serverTransport.accept();
                // 注册
                clientKey = client.registerSelector(selector, SelectionKey.OP_READ);

                // add this key to the map
                // 创建帧缓冲
                FrameBuffer frameBuffer = createFrameBuffer(client, clientKey, SelectAcceptThread.this);

                // 添加到 SelectionKey
                clientKey.attach(frameBuffer);
            } catch (TTransportException tte) {
                // something went wrong accepting.
                LOGGER.warn("Exception trying to accept!", tte);
                if (clientKey != null) cleanupSelectionKey(clientKey);
                if (client != null) client.close();
            }
        }
    } // SelectAcceptThread
}
