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

import org.apache.thrift.TAsyncProcessor;
import org.apache.thrift.TByteArrayOutputStream;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Provides common methods and classes used by nonblocking TServer
 * implementations.
 * 提供用于非阻塞 Server 实现的方法和类
 */
public abstract class AbstractNonblockingServer extends TServer {

    protected final Logger LOGGER = LoggerFactory.getLogger(getClass().getName());
    /**
     * The maximum amount of memory we will allocate to client IO buffers at a
     * time. Without this limit, the server will gladly allocate client buffers
     * right into an out of memory exception, rather than waiting.
     * <p>
     * 允许同时分配给客户端 IO 的最大内存，服务端更容易分配内存直到 OOM，而不是等待
     */
    final long MAX_READ_BUFFER_BYTES;

    /**
     * How many bytes are currently allocated to read buffers.
     * 可以并发读取的最大缓冲大小
     */
    final AtomicLong readBufferBytesAllocated = new AtomicLong(0);

    public AbstractNonblockingServer(AbstractNonblockingServerArgs args) {
        super(args);
        MAX_READ_BUFFER_BYTES = args.maxReadBufferBytes;
    }

    /**
     * Begin accepting connections and processing invocations.
     * 开始接收连接，处理调用
     */
    public void serve() {
        // start any IO threads
        // 启动
        if (!startThreads()) {
            return;
        }

        // start listening, or exit
        // 开始监听
        if (!startListening()) {
            return;
        }

        // 修改状态
        setServing(true);

        // this will block while we serve
        // 阻塞直到关闭
        waitForShutdown();

        setServing(false);

        // do a little cleanup
        // 停止监听器
        stopListening();
    }

    /**
     * Starts any threads required for serving.
     * 启动线程提供服务
     *
     * @return true if everything went ok, false if threads could not be started.
     * 如果正常则返回 true，如果不能启动则返回 false
     */
    protected abstract boolean startThreads();

    /**
     * A method that will block until when threads handling the serving have been
     * shut down.
     */
    protected abstract void waitForShutdown();

    /**
     * Have the server transport start accepting connections.
     * Server Transport 开始监听
     *
     * @return true if we started listening successfully, false if something went
     * wrong.
     * 如果启动监听成功则返回 true，否则返回 false
     */
    protected boolean startListening() {
        try {
            serverTransport_.listen();
            return true;
        } catch (TTransportException ttx) {
            LOGGER.error("Failed to start listening on server socket!", ttx);
            return false;
        }
    }

    /**
     * Stop listening for connections.
     * 停止监听连接
     */
    protected void stopListening() {
        serverTransport_.close();
    }

    /**
     * Perform an invocation. This method could behave several different ways -
     * invoke immediately inline, queue for separate execution, etc.
     * 执行处理，这个方法可能有多个实现方式：直接处理，加入队列等待执行等
     *
     * @return true if invocation was successfully requested, which is not a
     * guarantee that invocation has completed. False if the request
     * failed.
     * 如果调用成功则返回 true，这不能保证调用已完成，如果调用失败则返回 false
     */
    protected abstract boolean requestInvoke(FrameBuffer frameBuffer);

    /**
     * Possible states for the FrameBuffer state machine.
     * FrameBuffer 的状态机
     */
    private enum FrameBufferState {
        // in the midst of reading the frame size off the wire
        // 在读取到帧中间时
        READING_FRAME_SIZE,
        // reading the actual frame data now, but not all the way done yet
        // 真正读取帧数据，但是没有全部完成
        READING_FRAME,
        // completely read the frame, so an invocation can now happen
        // 读取完帧，可以开始调用
        READ_FRAME_COMPLETE,
        // waiting to get switched to listening for write events
        // 等待切换到监听
        AWAITING_REGISTER_WRITE,
        // started writing response data, not fully complete yet
        // 开始写入响应数据，但没有完成
        WRITING,
        // another thread wants this framebuffer to go back to reading
        // 另一个线程想要读取这个帧
        AWAITING_REGISTER_READ,
        // we want our transport and selection key invalidated in the selector
        // thread
        // 等待关闭
        AWAITING_CLOSE
    }

    public static abstract class AbstractNonblockingServerArgs<T extends AbstractNonblockingServerArgs<T>> extends AbstractServerArgs<T> {
        public long maxReadBufferBytes = 256 * 1024 * 1024;

        public AbstractNonblockingServerArgs(TNonblockingServerTransport transport) {
            super(transport);
            transportFactory(new TFramedTransport.Factory());
        }
    }

    /**
     * An abstract thread that handles selecting on a set of transports and
     * {@link FrameBuffer FrameBuffers} associated with selected keys
     * corresponding to requests.
     * <p>
     * 抽象的线程，用于处理 Transport 和相关联的 FrameBuffer 的请求
     */
    protected abstract class AbstractSelectThread extends Thread {

        // List of FrameBuffers that want to change their selection interests.
        protected final Set<FrameBuffer> selectInterestChanges = new HashSet<FrameBuffer>();

        protected Selector selector;

        public AbstractSelectThread() throws IOException {
            this.selector = SelectorProvider.provider().openSelector();
        }

        /**
         * If the selector is blocked, wake it up.
         * 如果线程被阻塞，则唤醒
         */
        public void wakeupSelector() {
            selector.wakeup();
        }

        /**
         * Add FrameBuffer to the list of select interest changes and wake up the
         * selector if it's blocked. When the select() call exits, it'll give the
         * FrameBuffer a chance to change its interests.
         * 将 FrameBuffer 添加到集合中，如果阻塞则唤醒，当调用存在时，FrameBuffer 可以改变
         * 关注的变化
         */
        public void requestSelectInterestChange(FrameBuffer frameBuffer) {
            synchronized (selectInterestChanges) {
                selectInterestChanges.add(frameBuffer);
            }
            // wakeup the selector, if it's currently blocked.
            selector.wakeup();
        }

        /**
         * Check to see if there are any FrameBuffers that have switched their
         * interest type from read to write or vice versa.
         * 检查是否有 FrameBuffer 将监听的改变从读取变为写入或者相反
         */
        protected void processInterestChanges() {
            synchronized (selectInterestChanges) {
                for (FrameBuffer fb : selectInterestChanges) {
                    fb.changeSelectInterests();
                }
                selectInterestChanges.clear();
            }
        }

        /**
         * Do the work required to read from a readable client. If the frame is
         * fully read, then invoke the method call.
         * 从可读的客户端读取，如果帧读取完成，则调用方法
         */
        protected void handleRead(SelectionKey key) {
            // 获取帧
            FrameBuffer buffer = (FrameBuffer) key.attachment();
            // 如果没有可读取的，则清理
            if (!buffer.read()) {
                cleanupSelectionKey(key);
                return;
            }

            // if the buffer's frame read is complete, invoke the method.
            // 如果 buffer 完全读取，则执行处理，如果失败则清理
            if (buffer.isFrameFullyRead()) {
                if (!requestInvoke(buffer)) {
                    cleanupSelectionKey(key);
                }
            }
        }

        /**
         * Let a writable client get written, if there's data to be written.
         * 如果有待写入数据向可写入的客户端写入
         */
        protected void handleWrite(SelectionKey key) {
            FrameBuffer buffer = (FrameBuffer) key.attachment();
            if (!buffer.write()) {
                cleanupSelectionKey(key);
            }
        }

        /**
         * Do connection-close cleanup on a given SelectionKey.
         * 将所给的 key 清理连接
         */
        protected void cleanupSelectionKey(SelectionKey key) {
            // remove the records from the two maps
            FrameBuffer buffer = (FrameBuffer) key.attachment();
            if (buffer != null) {
                // close the buffer
                buffer.close();
            }
            // cancel the selection key
            key.cancel();
        }
    } // SelectThread

    /**
     * Class that implements a sort of state machine around the interaction with a
     * client and an invoker. It manages reading the frame size and frame data,
     * getting it handed off as wrapped transports, and then the writing of
     * response data back to the client. In the process it manages flipping the
     * read and write bits on the selection key for its client.
     * <p>
     * 实现了对客户端调用感兴趣的状态机，管理帧大小和数据的读取，交给封装后的 Transport 处理，
     * 然后将响应数据返回给客户端，在此期间，管理数据的读取和写入
     */
    public class FrameBuffer {
        // the actual transport hooked up to the client.
        // 真正的与客户端联系的 Transport
        protected final TNonblockingTransport trans_;

        // the SelectionKey that corresponds to our transport
        // Transport 对应的 SelectionKey
        protected final SelectionKey selectionKey_;

        // the SelectThread that owns the registration of our transport
        // 持有注册的 Transport 的 SelectThread
        protected final AbstractSelectThread selectThread_;

        protected final TByteArrayOutputStream response_;

        // the frame that the TTransport should wrap.
        // Transport 应当封装的帧
        protected final TMemoryInputTransport frameTrans_;

        // the transport that should be used to connect to clients
        // 应当和客户端连接的 Transport
        protected final TTransport inTrans_;

        protected final TTransport outTrans_;

        // the input protocol to use on frames
        // 用于帧的输入的协议
        protected final TProtocol inProt_;

        // the output protocol to use on frames
        // 用于帧的输出的协议
        protected final TProtocol outProt_;

        // context associated with this connection
        // 连接相关的上下文
        protected final ServerContext context_;

        private final Logger LOGGER = LoggerFactory.getLogger(getClass().getName());

        // where in the process of reading/writing are we?
        // 状态
        protected FrameBufferState state_ = FrameBufferState.READING_FRAME_SIZE;

        // the ByteBuffer we'll be using to write and read, depending on the state
        // 用于写入和读取的 ByteBuffer，依赖状态
        protected ByteBuffer buffer_;

        public FrameBuffer(final TNonblockingTransport trans,
                           final SelectionKey selectionKey,
                           final AbstractSelectThread selectThread) {
            trans_ = trans;
            selectionKey_ = selectionKey;
            selectThread_ = selectThread;
            buffer_ = ByteBuffer.allocate(4);

            frameTrans_ = new TMemoryInputTransport();
            response_ = new TByteArrayOutputStream();
            inTrans_ = inputTransportFactory_.getTransport(frameTrans_);
            outTrans_ = outputTransportFactory_.getTransport(new TIOStreamTransport(response_));
            inProt_ = inputProtocolFactory_.getProtocol(inTrans_);
            outProt_ = outputProtocolFactory_.getProtocol(outTrans_);

            if (eventHandler_ != null) {
                context_ = eventHandler_.createContext(inProt_, outProt_);
            } else {
                context_ = null;
            }
        }

        /**
         * Give this FrameBuffer a chance to read. The selector loop should have
         * received a read event for this FrameBuffer.
         * 尝试 FrameBuffer 读取，Selector 循环应当会接收到这个 FramerBuffer 的读取事件
         *
         * @return true if the connection should live on, false if it should be
         * closed
         * 如果连接存活则返回 true，如果关闭则返回 false
         */
        public boolean read() {
            // 如果状态是读取中
            if (state_ == FrameBufferState.READING_FRAME_SIZE) {
                // try to read the frame size completely
                // 尝试完整的读取帧大小
                if (!internalRead()) {
                    return false;
                }

                // if the frame size has been read completely, then prepare to read the
                // actual frame.
                // 如果帧大小已经完全读取，则准备读取真正的帧
                if (buffer_.remaining() == 0) {
                    // pull out the frame size as an integer.
                    int frameSize = buffer_.getInt(0);
                    if (frameSize <= 0) {
                        LOGGER.error("Read an invalid frame size of " + frameSize
                                + ". Are you using TFramedTransport on the client side?");
                        return false;
                    }

                    // if this frame will always be too large for this server, log the
                    // error and close the connection.
                    if (frameSize > MAX_READ_BUFFER_BYTES) {
                        LOGGER.error("Read a frame size of " + frameSize
                                + ", which is bigger than the maximum allowable buffer size for ALL connections.");
                        return false;
                    }

                    // if this frame will push us over the memory limit, then return.
                    // with luck, more memory will free up the next time around.
                    // 如果帧大小超过限制，则返回
                    if (readBufferBytesAllocated.get() + frameSize > MAX_READ_BUFFER_BYTES) {
                        return true;
                    }

                    // increment the amount of memory allocated to read buffers
                    readBufferBytesAllocated.addAndGet(frameSize + 4);

                    // reallocate the readbuffer as a frame-sized buffer
                    // 读取数据帧
                    buffer_ = ByteBuffer.allocate(frameSize + 4);
                    buffer_.putInt(frameSize);

                    state_ = FrameBufferState.READING_FRAME;
                } else {
                    // this skips the check of READING_FRAME state below, since we can't
                    // possibly go on to that state if there's data left to be read at
                    // this one.
                    return true;
                }
            }

            // it is possible to fall through from the READING_FRAME_SIZE section
            // to READING_FRAME if there's already some frame data available once
            // READING_FRAME_SIZE is complete.
            // 如果状态是读取帧，则读取
            if (state_ == FrameBufferState.READING_FRAME) {
                if (!internalRead()) {
                    return false;
                }

                // since we're already in the select loop here for sure, we can just
                // modify our selection key directly.
                // 如果已经督导区完成，修改状态
                if (buffer_.remaining() == 0) {
                    // get rid of the read select interests
                    selectionKey_.interestOps(0);
                    state_ = FrameBufferState.READ_FRAME_COMPLETE;
                }

                return true;
            }

            // if we fall through to this point, then the state must be invalid.
            LOGGER.error("Read was called but state is invalid (" + state_ + ")");
            return false;
        }

        /**
         * Give this FrameBuffer a chance to write its output to the final client.
         * 尝试向 Transport 写入数据
         */
        public boolean write() {
            if (state_ == FrameBufferState.WRITING) {
                try {
                    // 写入
                    if (trans_.write(buffer_) < 0) {
                        return false;
                    }
                } catch (IOException e) {
                    LOGGER.warn("Got an IOException during write!", e);
                    return false;
                }

                // we're done writing. now we need to switch back to reading.
                // 如果没有待写入的，则切换到读取
                if (buffer_.remaining() == 0) {
                    prepareRead();
                }
                return true;
            }

            LOGGER.error("Write was called, but state is invalid (" + state_ + ")");
            return false;
        }

        /**
         * Give this FrameBuffer a chance to set its interest to write, once data
         * has come in.
         * 一旦有新的数据，尝试将 FrameBuffer 改为写入
         */
        public void changeSelectInterests() {
            switch (state_) {
                case AWAITING_REGISTER_WRITE:
                    // set the OP_WRITE interest
                    selectionKey_.interestOps(SelectionKey.OP_WRITE);
                    state_ = FrameBufferState.WRITING;
                    break;
                case AWAITING_REGISTER_READ:
                    prepareRead();
                    break;
                case AWAITING_CLOSE:
                    close();
                    selectionKey_.cancel();
                    break;
                default:
                    LOGGER.error("changeSelectInterest was called, but state is invalid ({})", state_);
            }
        }

        /**
         * Shut the connection down.
         * 关闭连接
         */
        public void close() {
            // if we're being closed due to an error, we might have allocated a
            // buffer that we need to subtract for our memory accounting.
            if (state_ == FrameBufferState.READING_FRAME ||
                    state_ == FrameBufferState.READ_FRAME_COMPLETE ||
                    state_ == FrameBufferState.AWAITING_CLOSE) {
                readBufferBytesAllocated.addAndGet(-buffer_.array().length);
            }
            trans_.close();
            if (eventHandler_ != null) {
                eventHandler_.deleteContext(context_, inProt_, outProt_);
            }
        }

        /**
         * Check if this FrameBuffer has a full frame read.
         * 检查是否完全读取帧
         */
        public boolean isFrameFullyRead() {
            return state_ == FrameBufferState.READ_FRAME_COMPLETE;
        }

        /**
         * After the processor has processed the invocation, whatever thread is
         * managing invocations should call this method on this FrameBuffer so we
         * know it's time to start trying to write again. Also, if it turns out that
         * there actually isn't any data in the response buffer, we'll skip trying
         * to write and instead go back to reading.
         * <p>
         * 当处理器处理完成后，无论哪个线程正在管理调用，都应在此FrameBuffer上调用此方法，
         * 因此我们知道该开始尝试再次编写了，另外，如果事实证明响应缓冲区中实际上没有任何数据，
         * 我们将跳过尝试写入的操作，而是返回读取操作
         */
        public void responseReady() {
            // the read buffer is definitely no longer in use, so we will decrement
            // our read buffer count. we do this here as well as in close because
            // we'd like to free this read memory up as quickly as possible for other
            // clients.
            readBufferBytesAllocated.addAndGet(-buffer_.array().length);

            if (response_.len() == 0) {
                // go straight to reading again. this was probably an oneway method
                state_ = FrameBufferState.AWAITING_REGISTER_READ;
                buffer_ = null;
            } else {
                buffer_ = ByteBuffer.wrap(response_.get(), 0, response_.len());

                // set state that we're waiting to be switched to write. we do this
                // asynchronously through requestSelectInterestChange() because there is
                // a possibility that we're not in the main thread, and thus currently
                // blocked in select(). (this functionality is in place for the sake of
                // the HsHa server.)
                state_ = FrameBufferState.AWAITING_REGISTER_WRITE;
            }
            requestSelectInterestChange();
        }

        /**
         * Actually invoke the method signified by this FrameBuffer.
         * 真正执行方法调用
         */
        public void invoke() {
            frameTrans_.reset(buffer_.array());
            response_.reset();

            try {
                // 如果有事件处理器，则触发
                if (eventHandler_ != null) {
                    eventHandler_.processContext(context_, inTrans_, outTrans_);
                }
                // 获取处理器，调用处理方法
                processorFactory_.getProcessor(inTrans_).process(inProt_, outProt_);
                responseReady();
                return;
            } catch (TException te) {
                LOGGER.warn("Exception while invoking!", te);
            } catch (Throwable t) {
                LOGGER.error("Unexpected throwable while invoking!", t);
            }
            // This will only be reached when there is a throwable.
            state_ = FrameBufferState.AWAITING_CLOSE;
            requestSelectInterestChange();
        }

        /**
         * Perform a read into buffer.
         * 尝试将 Transport 数据读取到缓冲区
         *
         * @return true if the read succeeded, false if there was an error or the
         * connection closed.
         */
        private boolean internalRead() {
            try {
                if (trans_.read(buffer_) < 0) {
                    return false;
                }
                return true;
            } catch (IOException e) {
                LOGGER.warn("Got an IOException in internalRead!", e);
                return false;
            }
        }

        /**
         * We're done writing, so reset our interest ops and change state
         * accordingly.
         * 写入完成，尝试将状态改为读取
         */
        private void prepareRead() {
            // we can set our interest directly without using the queue because
            // we're in the select thread.
            selectionKey_.interestOps(SelectionKey.OP_READ);
            // get ready for another go-around
            buffer_ = ByteBuffer.allocate(4);
            state_ = FrameBufferState.READING_FRAME_SIZE;
        }

        /**
         * When this FrameBuffer needs to change its select interests and execution
         * might not be in its select thread, then this method will make sure the
         * interest change gets done when the select thread wakes back up. When the
         * current thread is this FrameBuffer's select thread, then it just does the
         * interest change immediately.
         * <p>
         * 要求 FrameBuffer 的状态变化
         */
        protected void requestSelectInterestChange() {
            if (Thread.currentThread() == this.selectThread_) {
                changeSelectInterests();
            } else {
                this.selectThread_.requestSelectInterestChange(this);
            }
        }
    }

    // 异步的FrameBuffer
    public class AsyncFrameBuffer extends FrameBuffer {
        public AsyncFrameBuffer(TNonblockingTransport trans,
                                SelectionKey selectionKey,
                                AbstractSelectThread selectThread) {
            super(trans, selectionKey, selectThread);
        }

        public TProtocol getInputProtocol() {
            return inProt_;
        }

        public TProtocol getOutputProtocol() {
            return outProt_;
        }


        /**
         * 执行调用
         */
        public void invoke() {
            // 重置 Transport
            frameTrans_.reset(buffer_.array());
            response_.reset();

            try {
                // 触发事件
                if (eventHandler_ != null) {
                    eventHandler_.processContext(context_, inTrans_, outTrans_);
                }
                // 调用处理器处理
                ((TAsyncProcessor) processorFactory_.getProcessor(inTrans_)).process(this);
                return;
            } catch (TException te) {
                LOGGER.warn("Exception while invoking!", te);
            } catch (Throwable t) {
                LOGGER.error("Unexpected throwable while invoking!", t);
            }
            // This will only be reached when there is a throwable.
            // 修改状态
            state_ = FrameBufferState.AWAITING_CLOSE;
            requestSelectInterestChange();
        }
    }
}
