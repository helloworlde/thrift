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

package org.apache.thrift;

import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolDecorator;
import org.apache.thrift.protocol.TProtocolException;

import java.util.HashMap;
import java.util.Map;

/**
 * <code>TMultiplexedProcessor</code> is a <code>TProcessor</code> allowing
 * a single <code>TServer</code> to provide multiple services.
 * <p>
 * TMultiplexedProcessor 是 TProcessor，允许 TServer 提供多个服务
 *
 * <p>To do so, you instantiate the processor and then register additional
 * processors with it, as shown in the following example:</p>
 * 为了实现这个功能，需要先实例化处理器，然后注册其他的处理器
 *
 * <pre>
 * {@code
 *     TMultiplexedProcessor processor = new TMultiplexedProcessor();
 *
 *     processor.registerProcessor(
 *         "Calculator",
 *         new Calculator.Processor(new CalculatorHandler()));
 *
 *     processor.registerProcessor(
 *         "WeatherReport",
 *         new WeatherReport.Processor(new WeatherReportHandler()));
 *
 *     TServerTransport t = new TServerSocket(9090);
 *     TSimpleServer server = new TSimpleServer(processor, t);
 *
 *     server.serve();
 *     }
 * </pre>
 */
public class TMultiplexedProcessor implements TProcessor {

    private final Map<String, TProcessor> SERVICE_PROCESSOR_MAP = new HashMap<String, TProcessor>();

    private TProcessor defaultProcessor;

    /**
     * 'Register' a service with this <code>TMultiplexedProcessor</code>.  This
     * allows us to broker requests to individual services by using the service
     * name to select them at request time.
     * 向 TMultiplexedProcessor 注册服务，允许在请求时根据服务名称将请求路由到独立的服务上
     *
     * @param serviceName Name of a service, has to be identical to the name
     *                    declared in the Thrift IDL, e.g. "WeatherReport".
     *                    服务名称，必须和 IDL 文件声明的相同
     * @param processor   Implementation of a service, usually referred to
     *                    as "handlers", e.g. WeatherReportHandler implementing WeatherReport.Iface.
     *                    服务实现
     */
    public void registerProcessor(String serviceName, TProcessor processor) {
        SERVICE_PROCESSOR_MAP.put(serviceName, processor);
    }

    /**
     * Register a service to be called to process queries without service name
     * 注册默认的服务，如果没有服务名称则会默认使用
     *
     * @param processor
     */
    public void registerDefault(TProcessor processor) {
        defaultProcessor = processor;
    }

    /**
     * This implementation of <code>process</code> performs the following steps:
     * 处理请求
     *
     * <ol>
     *     <li>Read the beginning of the message.</li>
     *     <li>Extract the service name from the message.</li>
     *     <li>Using the service name to locate the appropriate processor.</li>
     *     <li>Dispatch to the processor, with a decorated instance of TProtocol
     *         that allows readMessageBegin() to return the original TMessage.</li>
     * </ol>
     * <p>
     * 1. 读取消息开始部分
     * 2. 获取服务名称
     * 3. 根据服务名称获取相应处理器
     * 4. 由 TProtocol 处理，通过 readMessageBegin() 读取并返回消息
     *
     * @throws TProtocolException If the message type is not CALL or ONEWAY, if
     *                            the service name was not found in the message, or if the service
     *                            name was not found in the service map.  You called {@link #registerProcessor(String, TProcessor) registerProcessor}
     *                            during initialization, right? :)
     *                            如果消息类型不是 CALL 或者 ONEWAY，或消息中没有服务名，或没有发现服务
     */
    public void process(TProtocol iprot, TProtocol oprot) throws TException {
        /*
            Use the actual underlying protocol (e.g. TBinaryProtocol) to read the
            message header.  This pulls the message "off the wire", which we'll
            deal with at the end of this method.
            使用底层的 Protocol 读取消息头，这将使消息"断开连接"，我们将在此方法结束时对其进行处理。
        */
        TMessage message = iprot.readMessageBegin();

        // 消息类型不对则抛出异常
        if (message.type != TMessageType.CALL && message.type != TMessageType.ONEWAY) {
            throw new TProtocolException(TProtocolException.NOT_IMPLEMENTED, "This should not have happened!?");
        }

        // Extract the service name
        // 获取服务名
        int index = message.name.indexOf(TMultiplexedProtocol.SEPARATOR);
        if (index < 0) {
            // 如果有默认的处理器，则由默认的处理器处理
            if (defaultProcessor != null) {
                // Dispatch processing to the stored processor
                defaultProcessor.process(new StoredMessageProtocol(iprot, message), oprot);
                return;
            }
            throw new TProtocolException(TProtocolException.NOT_IMPLEMENTED,
                    "Service name not found in message name: " + message.name + ".  Did you " +
                            "forget to use a TMultiplexProtocol in your client?");
        }

        // Create a new TMessage, something that can be consumed by any TProtocol
        // 获取服务名及处理器
        String serviceName = message.name.substring(0, index);
        TProcessor actualProcessor = SERVICE_PROCESSOR_MAP.get(serviceName);
        if (actualProcessor == null) {
            throw new TProtocolException(TProtocolException.NOT_IMPLEMENTED,
                    "Service name not found: " + serviceName + ".  Did you forget " +
                            "to call registerProcessor()?");
        }

        // Create a new TMessage, removing the service name
        // 构建消息
        TMessage standardMessage = new TMessage(
                message.name.substring(serviceName.length() + TMultiplexedProtocol.SEPARATOR.length()),
                message.type,
                message.seqid
        );

        // Dispatch processing to the stored processor
        // 处理请求
        actualProcessor.process(new StoredMessageProtocol(iprot, standardMessage), oprot);
    }

    /**
     * Our goal was to work with any protocol.  In order to do that, we needed
     * to allow them to call readMessageBegin() and get a TMessage in exactly
     * the standard format, without the service name prepended to TMessage.name.
     * <p>
     * 目标是在任意协议下都可以工作，允许调用 readMessageBegin() 获取 TMessage 的格式，
     * 无需在 Message 前面添加服务名
     */
    private static class StoredMessageProtocol extends TProtocolDecorator {
        TMessage messageBegin;

        public StoredMessageProtocol(TProtocol protocol, TMessage messageBegin) {
            super(protocol);
            this.messageBegin = messageBegin;
        }

        @Override
        public TMessage readMessageBegin() throws TException {
            return messageBegin;
        }
    }

}
