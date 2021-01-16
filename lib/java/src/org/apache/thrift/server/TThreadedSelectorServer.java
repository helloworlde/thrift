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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * A Half-Sync/Half-Async server with a separate pool of threads to handle
 * non-blocking I/O. Accepts are handled on a single thread, and a configurable
 * number of nonblocking selector threads manage reading and writing of client
 * connections. A synchronous worker thread pool handles processing of requests.
 * 支持使用单独的线程池处理非阻塞的 IO 的半同步半异步的 Server，连接由一个线程处理，可以配置
 * 用于处理与客户端连接读取和写入消息的线程数量，一个同步的线程池处理请求
 * <p>
 * Performs better than TNonblockingServer/THsHaServer in multi-core
 * environments when the the bottleneck is CPU on the single selector thread
 * handling I/O. In addition, because the accept handling is decoupled from
 * reads/writes and invocation, the server has better ability to handle back-
 * pressure from new connections (e.g. stop accepting when busy).
 * <p>
 * 当 CPU 成为单线程的 Selector 处理 IO 的瓶颈时，这个 Server 的性能要优于
 * TNonblockingServer 和 THsHaServer，同时，因为连接处理和读写处理是分离的，Server
 * 有更好的能力处理新连接的背压，如在繁忙时停止连接
 * <p>
 * Like TNonblockingServer, it relies on the use of TFramedTransport.
 * 和 TNonblockingServer 一样，依赖于 TFramedTransport 的使用
 */
public class TThreadedSelectorServer extends AbstractNonblockingServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(TThreadedSelectorServer.class.getName());
    // Threads handling events on client transports
    // 处理客户端 Transport 事件的线程
    private final Set<SelectorThread> selectorThreads = new HashSet<SelectorThread>();
    // This wraps all the functionality of queueing and thread pool management
    // for the passing of Invocations from the selector thread(s) to the workers
    // (if any).
    private final ExecutorService invoker;
    private final Args args;
    // The thread handling all accepts
    // 处理所有连接的线程
    private AcceptThread acceptThread;

    /**
     * Create the server with the specified Args configuration
     * 使用参数创建 Server
     */
    public TThreadedSelectorServer(Args args) {
        super(args);
        args.validate();
        invoker = args.executorService == null ? createDefaultExecutor(args) : args.executorService;
        this.args = args;
    }

    /**
     * Helper to create the invoker if one is not specified
     * 创建线程池
     */
    protected static ExecutorService createDefaultExecutor(Args options) {
        return (options.workerThreads > 0) ? Executors.newFixedThreadPool(options.workerThreads) : null;
    }

    /**
     * 创建处理连接的队列
     *
     * @param queueSize 队列大小
     * @return
     */
    private static BlockingQueue<TNonblockingTransport> createDefaultAcceptQueue(int queueSize) {
        // 如果队列大小为 0，则创建无界队列
        if (queueSize == 0) {
            // Unbounded queue
            return new LinkedBlockingQueue<TNonblockingTransport>();
        }
        // 创建有界队列
        return new ArrayBlockingQueue<TNonblockingTransport>(queueSize);
    }

    /**
     * Start the accept and selector threads running to deal with clients.
     * 启动连接和选择的线程，处理客户端事件
     *
     * @return true if everything went ok, false if we couldn't start for some
     * reason.
     * 如果正常则返回 true，否则返回 false
     */
    @Override
    protected boolean startThreads() {
        try {
            // 创建选择线程，并添加到集合中
            for (int i = 0; i < args.selectorThreads; ++i) {
                selectorThreads.add(new SelectorThread(args.acceptQueueSizePerThread));
            }
            // 创建处理连接的负载均衡， 创建处理连接的线程
            acceptThread = new AcceptThread((TNonblockingServerTransport) serverTransport_,
                    createSelectorThreadLoadBalancer(selectorThreads));

            // 启动选择线程
            for (SelectorThread thread : selectorThreads) {
                thread.start();
            }
            // 启动连接的线程
            acceptThread.start();
            return true;
        } catch (IOException e) {
            LOGGER.error("Failed to start threads!", e);
            return false;
        }
    }

    /**
     * Joins the accept and selector threads and shuts down the executor service.
     * 等待关闭
     */
    @Override
    protected void waitForShutdown() {
        try {
            joinThreads();
        } catch (InterruptedException e) {
            // Non-graceful shutdown occurred
            LOGGER.error("Interrupted while joining threads!", e);
        }
        gracefullyShutdownInvokerPool();
    }

    protected void joinThreads() throws InterruptedException {
        // wait until the io threads exit
        acceptThread.join();
        for (SelectorThread thread : selectorThreads) {
            thread.join();
        }
    }

    /**
     * Stop serving and shut everything down.
     * 停止服务，关闭所有的连接和线程
     */
    @Override
    public void stop() {
        stopped_ = true;

        // Stop queuing connect attempts asap
        // 停止连接
        stopListening();

        if (acceptThread != null) {
            // 清理选择器
            acceptThread.wakeupSelector();
        }
        if (selectorThreads != null) {
            // 遍历清理选择器线程
            for (SelectorThread thread : selectorThreads) {
                if (thread != null) {
                    thread.wakeupSelector();
                }
            }
        }
    }

    /**
     * 优雅关闭线程池
     */
    protected void gracefullyShutdownInvokerPool() {
        // try to gracefully shut down the executor service
        invoker.shutdown();

        // Loop until awaitTermination finally does return without a interrupted
        // exception. If we don't do this, then we'll shut down prematurely. We want
        // to let the executorService clear it's task queue, closing client sockets
        // appropriately.
        long timeoutMS = args.stopTimeoutUnit.toMillis(args.stopTimeoutVal);
        long now = System.currentTimeMillis();
        while (timeoutMS >= 0) {
            try {
                invoker.awaitTermination(timeoutMS, TimeUnit.MILLISECONDS);
                break;
            } catch (InterruptedException ix) {
                long newnow = System.currentTimeMillis();
                timeoutMS -= (newnow - now);
                now = newnow;
            }
        }
    }

    /**
     * We override the standard invoke method here to queue the invocation for
     * invoker service instead of immediately invoking. If there is no thread
     * pool, handle the invocation inline on this thread
     * <p>
     * 处理调用
     */
    @Override
    protected boolean requestInvoke(FrameBuffer frameBuffer) {
        // 封装为 Runnable
        Runnable invocation = getRunnable(frameBuffer);
        if (invoker != null) {
            try {
                // 执行处理
                invoker.execute(invocation);
                return true;
            } catch (RejectedExecutionException rx) {
                LOGGER.warn("ExecutorService rejected execution!", rx);
                return false;
            }
        } else {
            // Invoke on the caller's thread
            // 如果没有线程池，由当前线程直接处理
            invocation.run();
            return true;
        }
    }

    protected Runnable getRunnable(FrameBuffer frameBuffer) {
        return new Invocation(frameBuffer);
    }

    /**
     * Creates a SelectorThreadLoadBalancer to be used by the accept thread for
     * assigning newly accepted connections across the threads.
     */
    protected SelectorThreadLoadBalancer createSelectorThreadLoadBalancer(Collection<? extends SelectorThread> threads) {
        return new SelectorThreadLoadBalancer(threads);
    }

    /**
     * 构建参数
     */
    public static class Args extends AbstractNonblockingServerArgs<Args> {

        /**
         * The number of threads for selecting on already-accepted connections
         * 用于选择已经建立的连接的线程数量
         */
        public int selectorThreads = 2;
        /**
         * The size of the executor service (if none is specified) that will handle
         * invocations. This may be set to 0, in which case invocations will be
         * handled directly on the selector threads (as is in TNonblockingServer)
         * 用于处理调用的线程数量，可以设置为 0，调用将直接被 select 的线程处理，和 TNonblockingServer
         * 一样
         */
        private int workerThreads = 5;

        /**
         * Time to wait for server to stop gracefully
         * 等待 Server 优雅关闭的时间
         */
        private int stopTimeoutVal = 60;
        private TimeUnit stopTimeoutUnit = TimeUnit.SECONDS;

        /**
         * The ExecutorService for handling dispatched requests
         * 用于处理请求的线程池
         */
        private ExecutorService executorService = null;

        /**
         * The size of the blocking queue per selector thread for passing accepted
         * connections to the selector thread
         * <p>
         * 每个选择器线程的阻塞队列的大小，用于将接受的连接传递到选择器线程
         */
        private int acceptQueueSizePerThread = 4;
        private AcceptPolicy acceptPolicy = AcceptPolicy.FAST_ACCEPT;

        public Args(TNonblockingServerTransport transport) {
            super(transport);
        }

        public Args selectorThreads(int i) {
            selectorThreads = i;
            return this;
        }

        public int getSelectorThreads() {
            return selectorThreads;
        }

        public Args workerThreads(int i) {
            workerThreads = i;
            return this;
        }

        public int getWorkerThreads() {
            return workerThreads;
        }

        public int getStopTimeoutVal() {
            return stopTimeoutVal;
        }

        public Args stopTimeoutVal(int stopTimeoutVal) {
            this.stopTimeoutVal = stopTimeoutVal;
            return this;
        }

        public TimeUnit getStopTimeoutUnit() {
            return stopTimeoutUnit;
        }

        public Args stopTimeoutUnit(TimeUnit stopTimeoutUnit) {
            this.stopTimeoutUnit = stopTimeoutUnit;
            return this;
        }

        public ExecutorService getExecutorService() {
            return executorService;
        }

        public Args executorService(ExecutorService executorService) {
            this.executorService = executorService;
            return this;
        }

        public int getAcceptQueueSizePerThread() {
            return acceptQueueSizePerThread;
        }

        public Args acceptQueueSizePerThread(int acceptQueueSizePerThread) {
            this.acceptQueueSizePerThread = acceptQueueSizePerThread;
            return this;
        }

        public AcceptPolicy getAcceptPolicy() {
            return acceptPolicy;
        }

        public Args acceptPolicy(AcceptPolicy acceptPolicy) {
            this.acceptPolicy = acceptPolicy;
            return this;
        }

        public void validate() {
            if (selectorThreads <= 0) {
                throw new IllegalArgumentException("selectorThreads must be positive.");
            }
            if (workerThreads < 0) {
                throw new IllegalArgumentException("workerThreads must be non-negative.");
            }
            if (acceptQueueSizePerThread <= 0) {
                throw new IllegalArgumentException("acceptQueueSizePerThread must be positive.");
            }
        }

        /**
         * Determines the strategy for handling new accepted connections.
         * 处理新连接的策略
         */
        public static enum AcceptPolicy {
            /**
             * Require accepted connection registration to be handled by the executor.
             * If the worker pool is saturated, further accepts will be closed
             * immediately. Slightly increases latency due to an extra scheduling.
             * <p>
             * 要求接受的连接注册由执行者处理，如果线程池饱和，新的连接会被立即关闭，
             * 由于额外的调度，延迟略有增加
             */
            FAIR_ACCEPT,
            /**
             * Handle the accepts as fast as possible, disregarding the status of the
             * executor service.
             * 尽可能快的处理连接，无视线程池的状态
             */
            FAST_ACCEPT
        }
    }

    /**
     * A round robin load balancer for choosing selector threads for new
     * connections.
     * 用于处理新连接时选择 Selector 线程的轮询的负载均衡器
     */
    protected static class SelectorThreadLoadBalancer {

        private final Collection<? extends SelectorThread> threads;

        private Iterator<? extends SelectorThread> nextThreadIterator;

        public <T extends SelectorThread> SelectorThreadLoadBalancer(Collection<T> threads) {
            if (threads.isEmpty()) {
                throw new IllegalArgumentException("At least one selector thread is required");
            }
            this.threads = Collections.unmodifiableList(new ArrayList<T>(threads));
            nextThreadIterator = this.threads.iterator();
        }

        /**
         * 通过迭代器选择下一个
         */
        public SelectorThread nextThread() {
            // Choose a selector thread (round robin)
            if (!nextThreadIterator.hasNext()) {
                nextThreadIterator = threads.iterator();
            }
            return nextThreadIterator.next();
        }
    }

    /**
     * The thread that selects on the server transport (listen socket) and accepts
     * new connections to hand off to the IO selector threads
     * <p>
     * 在服务器传输中选择线程（监听 Socket）并接受新连接以移交给 IO 选择器线程
     */
    protected class AcceptThread extends Thread {

        // The listen socket to accept on
        // 监听的端口
        private final TNonblockingServerTransport serverTransport;
        // 选择器
        private final Selector acceptSelector;

        // 选择线程负载均衡器
        private final SelectorThreadLoadBalancer threadChooser;

        /**
         * Set up the AcceptThead
         * 创建处理连接的线程
         *
         * @throws IOException
         */
        public AcceptThread(TNonblockingServerTransport serverTransport,
                            SelectorThreadLoadBalancer threadChooser) throws IOException {
            this.serverTransport = serverTransport;
            this.threadChooser = threadChooser;
            this.acceptSelector = SelectorProvider.provider().openSelector();
            this.serverTransport.registerSelector(acceptSelector);
        }

        /**
         * The work loop. Selects on the server transport and accepts. If there was
         * a server transport that had blocking accepts, and returned on blocking
         * client transports, that should be used instead
         * <p>
         * 工作循环，选择并建立连接，如果有 Server Transport 阻塞连接，并且在阻塞的客户端 Transport
         * 返回，应当被代替
         */
        public void run() {
            try {
                if (eventHandler_ != null) {
                    // 通知 Server 开始启动
                    eventHandler_.preServe();
                }

                while (!stopped_) {
                    // 选择处理连接
                    select();
                }
            } catch (Throwable t) {
                LOGGER.error("run() on AcceptThread exiting due to uncaught error", t);
            } finally {
                try {
                    acceptSelector.close();
                } catch (IOException e) {
                    LOGGER.error("Got an IOException while closing accept selector!", e);
                }
                // This will wake up the selector threads
                TThreadedSelectorServer.this.stop();
            }
        }

        /**
         * If the selector is blocked, wake it up.
         */
        public void wakeupSelector() {
            acceptSelector.wakeup();
        }

        /**
         * Select and process IO events appropriately: If there are connections to
         * be accepted, accept them.
         * <p>
         * 选择并适当处理 IO 事件，如果有连接等待，则建立连接
         */
        private void select() {
            try {
                // wait for connect events.
                // 等待连接事件
                acceptSelector.select();

                // process the io events we received
                // 处理接收到的事件
                Iterator<SelectionKey> selectedKeys = acceptSelector.selectedKeys().iterator();
                while (!stopped_ && selectedKeys.hasNext()) {
                    SelectionKey key = selectedKeys.next();
                    selectedKeys.remove();

                    // skip if not valid
                    if (!key.isValid()) {
                        continue;
                    }

                    if (key.isAcceptable()) {
                        // 建立连接
                        handleAccept();
                    } else {
                        LOGGER.warn("Unexpected state in select! " + key.interestOps());
                    }
                }
            } catch (IOException e) {
                LOGGER.warn("Got an IOException while selecting!", e);
            }
        }

        /**
         * Accept a new connection.
         * 建立新的连接
         */
        private void handleAccept() {
            // 建立连接
            final TNonblockingTransport client = doAccept();
            if (client != null) {
                // Pass this connection to a selector thread
                // 将连接传递给选择线程
                final SelectorThread targetThread = threadChooser.nextThread();

                // 如果策略是尽快建立连接，则添加到处理的队列中
                if (args.acceptPolicy == Args.AcceptPolicy.FAST_ACCEPT || invoker == null) {
                    doAddAccept(targetThread, client);
                } else {
                    // FAIR_ACCEPT
                    try {
                        // 如果是 FAIR_ACCEPT，则提交异步任务进行添加
                        invoker.submit(new Runnable() {
                            public void run() {
                                doAddAccept(targetThread, client);
                            }
                        });
                    } catch (RejectedExecutionException rx) {
                        LOGGER.warn("ExecutorService rejected accept registration!", rx);
                        // close immediately
                        client.close();
                    }
                }
            }
        }

        /**
         * 建立连接
         */
        private TNonblockingTransport doAccept() {
            try {
                return (TNonblockingTransport) serverTransport.accept();
            } catch (TTransportException tte) {
                // something went wrong accepting.
                LOGGER.warn("Exception trying to accept!", tte);
                return null;
            }
        }

        /**
         * 添加连接到处理的线程中
         */
        private void doAddAccept(SelectorThread thread, TNonblockingTransport client) {
            if (!thread.addAcceptedConnection(client)) {
                client.close();
            }
        }
    } // AcceptThread

    /**
     * The SelectorThread(s) will be doing all the selecting on accepted active
     * connections.
     * <p>
     * SelectorThread 在建立的连接上进行选择
     */
    protected class SelectorThread extends AbstractSelectThread {

        // Accepted connections added by the accept thread.
        private final BlockingQueue<TNonblockingTransport> acceptedQueue;
        private int SELECTOR_AUTO_REBUILD_THRESHOLD = 512;
        private long MONITOR_PERIOD = 1000L;
        private int jvmBug = 0;

        /**
         * Set up the SelectorThread with an unbounded queue for incoming accepts.
         *
         * @throws IOException if a selector cannot be created
         */
        public SelectorThread() throws IOException {
            this(new LinkedBlockingQueue<TNonblockingTransport>());
        }

        /**
         * Set up the SelectorThread with an bounded queue for incoming accepts.
         * 创建用于保存连接的有界队列并创建线程
         *
         * @throws IOException if a selector cannot be created
         */
        public SelectorThread(int maxPendingAccepts) throws IOException {
            this(createDefaultAcceptQueue(maxPendingAccepts));
        }

        /**
         * Set up the SelectorThread with a specified queue for connections.
         *
         * @param acceptedQueue The BlockingQueue implementation for holding incoming accepted
         *                      connections.
         * @throws IOException if a selector cannot be created.
         */
        public SelectorThread(BlockingQueue<TNonblockingTransport> acceptedQueue) throws IOException {
            this.acceptedQueue = acceptedQueue;
        }

        /**
         * Hands off an accepted connection to be handled by this thread. This
         * method will block if the queue for new connections is at capacity.
         * 处理这个线程要处理的建立的连接，如果队列容量满了则会阻塞
         *
         * @param accepted The connection that has been accepted.
         *                 建立的连接
         * @return true if the connection has been successfully added.
         * 如果添加成功则返回 true
         */
        public boolean addAcceptedConnection(TNonblockingTransport accepted) {
            try {
                // 放入队列中
                acceptedQueue.put(accepted);
            } catch (InterruptedException e) {
                LOGGER.warn("Interrupted while adding accepted connection!", e);
                return false;
            }
            selector.wakeup();
            return true;
        }

        /**
         * The work loop. Handles selecting (read/write IO), dispatching, and
         * managing the selection preferences of all existing connections.
         * 工作循环，处理 IO，分发并管理所有存在的连接
         */
        public void run() {
            try {
                while (!stopped_) {
                    // 选择读取或写入事件
                    select();
                    // 处理新的连接
                    processAcceptedConnections();
                    // 改变需要改变的状态
                    processInterestChanges();
                }

                // 如果停止了，则清理选择
                for (SelectionKey selectionKey : selector.keys()) {
                    cleanupSelectionKey(selectionKey);
                }
            } catch (Throwable t) {
                LOGGER.error("run() on SelectorThread exiting due to uncaught error", t);
            } finally {
                try {
                    // 关闭
                    selector.close();
                } catch (IOException e) {
                    LOGGER.error("Got an IOException while closing selector!", e);
                }
                // This will wake up the accept thread and the other selector threads
                TThreadedSelectorServer.this.stop();
            }
        }

        /**
         * Select and process IO events appropriately: If there are existing
         * connections with data waiting to be read, read it, buffering until a
         * whole frame has been read. If there are any pending responses, buffer
         * them until their target client is available, and then send the data.
         * <p>
         * 选择并适当处理 IO 事件，如果存在的连接有代读取的数据，缓冲直到整个帧被读取，如果
         * 有等待响应的，缓冲直到客户端可用，然后发送数据
         */
        private void select() {
            try {

                // 获取事件
                doSelect();

                // process the io events we received
                Iterator<SelectionKey> selectedKeys = selector.selectedKeys().iterator();
                while (!stopped_ && selectedKeys.hasNext()) {
                    SelectionKey key = selectedKeys.next();
                    selectedKeys.remove();

                    // skip if not valid
                    // 如果无效则跳过
                    if (!key.isValid()) {
                        cleanupSelectionKey(key);
                        continue;
                    }

                    if (key.isReadable()) {
                        // deal with reads
                        // 如果是读取则处理读取事件
                        handleRead(key);
                    } else if (key.isWritable()) {
                        // deal with writes
                        // 如果是写入则处理写入
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
         * Do select and judge epoll bug happen.
         * See : https://issues.apache.org/jira/browse/THRIFT-4251
         * <p>
         * 选择事件
         */
        private void doSelect() throws IOException {
            long beforeSelect = System.currentTimeMillis();
            int selectedNums = selector.select();
            long afterSelect = System.currentTimeMillis();

            if (selectedNums == 0) {
                jvmBug++;
            } else {
                jvmBug = 0;
            }

            long selectedTime = afterSelect - beforeSelect;
            if (selectedTime >= MONITOR_PERIOD) {
                jvmBug = 0;
            } else if (jvmBug > SELECTOR_AUTO_REBUILD_THRESHOLD) {
                LOGGER.warn("In {} ms happen {} times jvm bug; rebuilding selector.", MONITOR_PERIOD, jvmBug);
                rebuildSelector();
                selector.selectNow();
                jvmBug = 0;
            }

        }

        /**
         * Replaces the current Selector of this SelectorThread with newly created Selector to work
         * around the infamous epoll 100% CPU bug.
         * <p>
         * 重新构建 Selector
         */
        private synchronized void rebuildSelector() {
            final Selector oldSelector = selector;
            if (oldSelector == null) {
                return;
            }
            Selector newSelector = null;
            try {
                newSelector = Selector.open();
                LOGGER.warn("Created new Selector.");
            } catch (IOException e) {
                LOGGER.error("Create new Selector error.", e);
            }

            for (SelectionKey key : oldSelector.selectedKeys()) {
                if (!key.isValid() && key.readyOps() == 0) {
                    continue;
                }

                SelectableChannel channel = key.channel();
                Object attachment = key.attachment();

                try {
                    if (attachment == null) {
                        channel.register(newSelector, key.readyOps());
                    } else {
                        channel.register(newSelector, key.readyOps(), attachment);
                    }
                } catch (ClosedChannelException e) {
                    LOGGER.error("Register new selector key error.", e);
                }

            }

            selector = newSelector;
            try {
                oldSelector.close();
            } catch (IOException e) {
                LOGGER.error("Close old selector error.", e);
            }
            LOGGER.warn("Replace new selector success.");
        }

        /**
         * 处理新的连接
         */
        private void processAcceptedConnections() {
            // Register accepted connections
            while (!stopped_) {
                // 获取连接
                TNonblockingTransport accepted = acceptedQueue.poll();
                if (accepted == null) {
                    break;
                }
                // 注册
                registerAccepted(accepted);
            }
        }

        /**
         * 创建 FrameBuffer
         */
        protected FrameBuffer createFrameBuffer(final TNonblockingTransport trans,
                                                final SelectionKey selectionKey,
                                                final AbstractSelectThread selectThread) {
            return processorFactory_.isAsyncProcessor() ?
                    new AsyncFrameBuffer(trans, selectionKey, selectThread) :
                    new FrameBuffer(trans, selectionKey, selectThread);
        }

        /**
         * 注册连接
         *
         * @param accepted
         */
        private void registerAccepted(TNonblockingTransport accepted) {
            SelectionKey clientKey = null;
            try {
                // 注册选择器
                clientKey = accepted.registerSelector(selector, SelectionKey.OP_READ);

                // 创建 FrameBuffer
                FrameBuffer frameBuffer = createFrameBuffer(accepted, clientKey, SelectorThread.this);
                // 添加 FrameBuffer
                clientKey.attach(frameBuffer);
            } catch (IOException e) {
                LOGGER.warn("Failed to register accepted connection to selector!", e);
                if (clientKey != null) {
                    cleanupSelectionKey(clientKey);
                }
                accepted.close();
            }
        }
    } // SelectorThread
}
