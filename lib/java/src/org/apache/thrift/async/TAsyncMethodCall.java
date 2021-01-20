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
package org.apache.thrift.async;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Encapsulates an async method call.
 * 封装的异步方法调用
 * <p>
 * Need to generate:
 * <ul>
 *   <li>protected abstract void write_args(TProtocol protocol)</li>
 *   <li>protected abstract T getResult() throws &lt;Exception_1&gt;, &lt;Exception_2&gt;, ...</li>
 * </ul>
 *
 * @param <T> The return type of the encapsulated method call.
 */
public abstract class TAsyncMethodCall<T> {

    private static final int INITIAL_MEMORY_BUFFER_SIZE = 128;

    private static AtomicLong sequenceIdCounter = new AtomicLong(0);
    protected final TNonblockingTransport transport;
    protected final TAsyncClient client;
    private final TProtocolFactory protocolFactory;
    private final AsyncMethodCallback<T> callback;
    private final boolean isOneway;
    private final long timeout;
    private final byte[] sizeBufferArray = new byte[4];
    /**
     * Next step in the call, initialized by start()
     */
    private State state = null;
    private long sequenceId;
    private ByteBuffer sizeBuffer;
    private ByteBuffer frameBuffer;
    private long startTime = System.currentTimeMillis();

    protected TAsyncMethodCall(TAsyncClient client,
                               TProtocolFactory protocolFactory,
                               TNonblockingTransport transport,
                               AsyncMethodCallback<T> callback,
                               boolean isOneway) {
        this.transport = transport;
        this.callback = callback;
        this.protocolFactory = protocolFactory;
        this.client = client;
        this.isOneway = isOneway;
        this.sequenceId = TAsyncMethodCall.sequenceIdCounter.getAndIncrement();
        this.timeout = client.getTimeout();
    }

    protected State getState() {
        return state;
    }

    protected boolean isFinished() {
        return state == State.RESPONSE_READ;
    }

    protected long getStartTime() {
        return startTime;
    }

    protected long getSequenceId() {
        return sequenceId;
    }

    public TAsyncClient getClient() {
        return client;
    }

    public boolean hasTimeout() {
        return timeout > 0;
    }

    public long getTimeoutTimestamp() {
        return timeout + startTime;
    }

    protected abstract void write_args(TProtocol protocol) throws TException;

    protected abstract T getResult() throws Exception;

    /**
     * Initialize buffers.
     * 初始化缓冲区
     *
     * @throws TException if buffer initialization fails
     */
    protected void prepareMethodCall() throws TException {
        TMemoryBuffer memoryBuffer = new TMemoryBuffer(INITIAL_MEMORY_BUFFER_SIZE);
        TProtocol protocol = protocolFactory.getProtocol(memoryBuffer);
        write_args(protocol);

        int length = memoryBuffer.length();
        frameBuffer = ByteBuffer.wrap(memoryBuffer.getArray(), 0, length);

        TFramedTransport.encodeFrameSize(length, sizeBufferArray);
        sizeBuffer = ByteBuffer.wrap(sizeBufferArray);
    }

    /**
     * Register with selector and start first state, which could be either connecting or writing.
     * 注册 Selector，开始第一个状态，既可以用于连接，也可以用于写入
     *
     * @throws IOException if register or starting fails
     */
    void start(Selector sel) throws IOException {
        SelectionKey key;
        // 如果 Transport 是开启的，则修改状态为 WRITING_REQUEST_SIZE，注册写操作
        if (transport.isOpen()) {
            state = State.WRITING_REQUEST_SIZE;
            key = transport.registerSelector(sel, SelectionKey.OP_WRITE);
        } else {
            // Transport 状态不是开启的，修改状态为 CONNECTING，注册连接操作
            state = State.CONNECTING;
            key = transport.registerSelector(sel, SelectionKey.OP_CONNECT);

            // non-blocking connect can complete immediately,
            // in which case we should not expect the OP_CONNECT
            // 开启连接后注册写入
            if (transport.startConnect()) {
                registerForFirstWrite(key);
            }
        }

        key.attach(this);
    }

    /**
     * 注册写操作
     *
     * @param key
     * @throws IOException
     */
    void registerForFirstWrite(SelectionKey key) throws IOException {
        state = State.WRITING_REQUEST_SIZE;
        key.interestOps(SelectionKey.OP_WRITE);
    }

    protected ByteBuffer getFrameBuffer() {
        return frameBuffer;
    }

    /**
     * Transition to next state, doing whatever work is required. Since this
     * method is only called by the selector thread, we can make changes to our
     * select interests without worrying about concurrency.
     * <p>
     * 过渡到下一个状态，做任何需要的工作，因为这个方法只有有 Selector 的线程调用，可以修改状态
     * 无需关心并发
     *
     * @param key
     */
    void transition(SelectionKey key) {
        // Ensure key is valid
        if (!key.isValid()) {
            key.cancel();
            Exception e = new TTransportException("Selection key not valid!");
            onError(e);
            return;
        }

        // Transition function
        try {
            switch (state) {
                case CONNECTING:
                    doConnecting(key);
                    break;
                case WRITING_REQUEST_SIZE:
                    doWritingRequestSize();
                    break;
                case WRITING_REQUEST_BODY:
                    doWritingRequestBody(key);
                    break;
                case READING_RESPONSE_SIZE:
                    doReadingResponseSize();
                    break;
                case READING_RESPONSE_BODY:
                    doReadingResponseBody(key);
                    break;
                default: // RESPONSE_READ, ERROR, or bug
                    throw new IllegalStateException("Method call in state " + state
                            + " but selector called transition method. Seems like a bug...");
            }
        } catch (Exception e) {
            key.cancel();
            key.attach(null);
            onError(e);
        }
    }

    protected void onError(Exception e) {
        client.onError(e);
        callback.onError(e);
        state = State.ERROR;
    }

    /**
     * 读取响应结果
     *
     * @param key
     * @throws IOException
     */
    private void doReadingResponseBody(SelectionKey key) throws IOException {
        if (transport.read(frameBuffer) < 0) {
            throw new IOException("Read call frame failed");
        }
        if (frameBuffer.remaining() == 0) {
            cleanUpAndFireCallback(key);
        }
    }

    /**
     * 清理并执行回调
     *
     * @param key
     */
    private void cleanUpAndFireCallback(SelectionKey key) {
        state = State.RESPONSE_READ;
        key.interestOps(0);
        // this ensures that the TAsyncMethod instance doesn't hang around
        key.attach(null);
        try {
            T result = this.getResult();
            client.onComplete();
            callback.onComplete(result);
        } catch (Exception e) {
            key.cancel();
            onError(e);
        }
    }

    /**
     * 读取响应大小
     *
     * @throws IOException
     */
    private void doReadingResponseSize() throws IOException {
        if (transport.read(sizeBuffer) < 0) {
            throw new IOException("Read call frame size failed");
        }
        if (sizeBuffer.remaining() == 0) {
            state = State.READING_RESPONSE_BODY;
            frameBuffer = ByteBuffer.allocate(TFramedTransport.decodeFrameSize(sizeBufferArray));
        }
    }

    /**
     * 写入请求内容
     *
     * @param key
     * @throws IOException
     */
    private void doWritingRequestBody(SelectionKey key) throws IOException {
        if (transport.write(frameBuffer) < 0) {
            throw new IOException("Write call frame failed");
        }
        if (frameBuffer.remaining() == 0) {
            if (isOneway) {
                cleanUpAndFireCallback(key);
            } else {
                state = State.READING_RESPONSE_SIZE;
                sizeBuffer.rewind();  // Prepare to read incoming frame size
                key.interestOps(SelectionKey.OP_READ);
            }
        }
    }

    /**
     * 写入请求大小
     *
     * @throws IOException
     */
    private void doWritingRequestSize() throws IOException {
        if (transport.write(sizeBuffer) < 0) {
            throw new IOException("Write call frame size failed");
        }
        if (sizeBuffer.remaining() == 0) {
            state = State.WRITING_REQUEST_BODY;
        }
    }

    /**
     * 建立连接
     *
     * @param key
     * @throws IOException
     */
    private void doConnecting(SelectionKey key) throws IOException {
        if (!key.isConnectable() || !transport.finishConnect()) {
            throw new IOException("not connectable or finishConnect returned false after we got an OP_CONNECT");
        }
        registerForFirstWrite(key);
    }

    public static enum State {
        CONNECTING,
        WRITING_REQUEST_SIZE,
        WRITING_REQUEST_BODY,
        READING_RESPONSE_SIZE,
        READING_RESPONSE_BODY,
        RESPONSE_READ,
        ERROR;
    }
}
