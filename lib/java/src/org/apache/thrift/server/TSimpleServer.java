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

import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple singlethreaded server for testing.
 * 用于测试的简单的单线程 Server
 */
public class TSimpleServer extends TServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(TSimpleServer.class.getName());

    public TSimpleServer(AbstractServerArgs args) {
        super(args);
    }

    /**
     * 启动并开始提供服务
     */
    public void serve() {
        try {
            // 监听 Socket
            serverTransport_.listen();
        } catch (TTransportException ttx) {
            LOGGER.error("Error occurred during listening.", ttx);
            return;
        }

        // Run the preServe event
        // 如果有事件处理器，则调用其 preSever 方法
        if (eventHandler_ != null) {
            eventHandler_.preServe();
        }

        // 设置运行状态
        setServing(true);

        // 只要没有关闭，就获取连接
        while (!stopped_) {
            TTransport client = null;
            TProcessor processor = null;
            TTransport inputTransport = null;
            TTransport outputTransport = null;
            TProtocol inputProtocol = null;
            TProtocol outputProtocol = null;
            ServerContext connectionContext = null;
            try {
                // 接受连接
                client = serverTransport_.accept();
                if (client != null) {
                    // 初始化相关的资源
                    processor = processorFactory_.getProcessor(client);
                    inputTransport = inputTransportFactory_.getTransport(client);
                    outputTransport = outputTransportFactory_.getTransport(client);
                    inputProtocol = inputProtocolFactory_.getProtocol(inputTransport);
                    outputProtocol = outputProtocolFactory_.getProtocol(outputTransport);
                    // 如果有事件监听器，则触发创建上下文事件
                    if (eventHandler_ != null) {
                        connectionContext = eventHandler_.createContext(inputProtocol, outputProtocol);
                    }
                    while (true) {
                        // 处理上下文事件
                        if (eventHandler_ != null) {
                            eventHandler_.processContext(connectionContext, inputTransport, outputTransport);
                        }
                        // 处理请求
                        processor.process(inputProtocol, outputProtocol);
                    }
                }
            } catch (TTransportException ttx) {
                // Client died, just move on
            } catch (TException tx) {
                if (!stopped_) {
                    LOGGER.error("Thrift error occurred during processing of message.", tx);
                }
            } catch (Exception x) {
                if (!stopped_) {
                    LOGGER.error("Error occurred during processing of message.", x);
                }
            }

            // 上下文删除事件
            if (eventHandler_ != null) {
                eventHandler_.deleteContext(connectionContext, inputProtocol, outputProtocol);
            }

            // 关闭 Transport
            if (inputTransport != null) {
                inputTransport.close();
            }

            if (outputTransport != null) {
                outputTransport.close();
            }

        }
        // 修改服务状态
        setServing(false);
    }

    /**
     * 停止服务，打断 Transport
     */
    public void stop() {
        stopped_ = true;
        serverTransport_.interrupt();
    }
}
