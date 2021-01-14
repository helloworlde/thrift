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

import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportFactory;

/**
 * Generic interface for a Thrift server.
 * Thrift Server 泛化接口
 */
public abstract class TServer {

    /**
     * Core processor
     * 核心处理器
     */
    protected TProcessorFactory processorFactory_;
    /**
     * Server transport
     * 服务端 Transport
     */
    protected TServerTransport serverTransport_;
    /**
     * Input Transport Factory
     * 输入 Transport 工厂
     */
    protected TTransportFactory inputTransportFactory_;
    /**
     * Output Transport Factory
     * 输出 Transport 工厂
     */
    protected TTransportFactory outputTransportFactory_;
    /**
     * Input Protocol Factory
     * 输入协议工厂
     */
    protected TProtocolFactory inputProtocolFactory_;
    /**
     * Output Protocol Factory
     * 输出协议工厂
     */
    protected TProtocolFactory outputProtocolFactory_;
    // Server 端事件处理器
    protected TServerEventHandler eventHandler_;
    // Flag for stopping the server
    // Please see THRIFT-1795 for the usage of this flag
    // Server 端停止标识
    protected volatile boolean stopped_ = false;
    // 是否在提供服务
    private volatile boolean isServing;

    /**
     * 构建 Server
     *
     * @param args 构建参数
     */
    protected TServer(AbstractServerArgs args) {
        processorFactory_ = args.processorFactory;
        serverTransport_ = args.serverTransport;
        inputTransportFactory_ = args.inputTransportFactory;
        outputTransportFactory_ = args.outputTransportFactory;
        inputProtocolFactory_ = args.inputProtocolFactory;
        outputProtocolFactory_ = args.outputProtocolFactory;
    }

    /**
     * The run method fires up the server and gets things going.
     * run 方法启动 Server 并开始提供服务
     */
    public abstract void serve();

    /**
     * Stop the server. This is optional on a per-implementation basis. Not
     * all servers are required to be cleanly stoppable.
     * 停止 Server，每个实现是可选的，并不是所有的 Server 都要求清除并停止
     */
    public void stop() {
    }

    public boolean isServing() {
        return isServing;
    }

    protected void setServing(boolean serving) {
        isServing = serving;
    }

    public void setServerEventHandler(TServerEventHandler eventHandler) {
        eventHandler_ = eventHandler;
    }

    public TServerEventHandler getEventHandler() {
        return eventHandler_;
    }

    public boolean getShouldStop() {
        return this.stopped_;
    }

    public void setShouldStop(boolean shouldStop) {
        this.stopped_ = shouldStop;
    }

    /**
     * 构造参数
     */
    public static class Args extends AbstractServerArgs<Args> {
        public Args(TServerTransport transport) {
            super(transport);
        }
    }

    /**
     * 构造参数
     */
    public static abstract class AbstractServerArgs<T extends AbstractServerArgs<T>> {

        // Server Transport
        final TServerTransport serverTransport;

        // 处理工厂
        TProcessorFactory processorFactory;
        // 输入的 Transport 工厂
        TTransportFactory inputTransportFactory = new TTransportFactory();
        // 输出的 Transport 工厂
        TTransportFactory outputTransportFactory = new TTransportFactory();
        // 输入的协议工厂
        TProtocolFactory inputProtocolFactory = new TBinaryProtocol.Factory();
        // 输出的协议工厂
        TProtocolFactory outputProtocolFactory = new TBinaryProtocol.Factory();

        public AbstractServerArgs(TServerTransport transport) {
            serverTransport = transport;
        }

        public T processorFactory(TProcessorFactory factory) {
            this.processorFactory = factory;
            return (T) this;
        }

        public T processor(TProcessor processor) {
            this.processorFactory = new TProcessorFactory(processor);
            return (T) this;
        }

        public T transportFactory(TTransportFactory factory) {
            this.inputTransportFactory = factory;
            this.outputTransportFactory = factory;
            return (T) this;
        }

        public T inputTransportFactory(TTransportFactory factory) {
            this.inputTransportFactory = factory;
            return (T) this;
        }

        public T outputTransportFactory(TTransportFactory factory) {
            this.outputTransportFactory = factory;
            return (T) this;
        }

        public T protocolFactory(TProtocolFactory factory) {
            this.inputProtocolFactory = factory;
            this.outputProtocolFactory = factory;
            return (T) this;
        }

        public T inputProtocolFactory(TProtocolFactory factory) {
            this.inputProtocolFactory = factory;
            return (T) this;
        }

        public T outputProtocolFactory(TProtocolFactory factory) {
            this.outputProtocolFactory = factory;
            return (T) this;
        }
    }
}
