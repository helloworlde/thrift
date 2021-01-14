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

import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

/**
 * Interface that can handle events from the server core. To
 * use this you should subclass it and implement the methods that you care
 * about. Your subclass can also store local data that you may care about,
 * such as additional "arguments" to these methods (stored in the object
 * instance's state).
 * <p>
 * Server 端事件处理器接口
 */
public interface TServerEventHandler {

    /**
     * Called before the server begins.
     * 在 Server 开始调用之前调用
     */
    void preServe();

    /**
     * Called when a new client has connected and is about to being processing.
     * 当有新的客户端连接，并将要处理时调用
     */
    ServerContext createContext(TProtocol input,
                                TProtocol output);

    /**
     * Called when a client has finished request-handling to delete server
     * context.
     * 当客户端请求处理完成，删除 Server 端 Context
     */
    void deleteContext(ServerContext serverContext,
                       TProtocol input,
                       TProtocol output);

    /**
     * Called when a client is about to call the processor.
     * 当客户端开始调用处理器时使用
     */
    void processContext(ServerContext serverContext,
                        TTransport inputTransport,
                        TTransport outputTransport);

}
