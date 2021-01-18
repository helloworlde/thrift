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
import org.apache.thrift.protocol.TProtocol;

/**
 * A TServiceClient is used to communicate with a TService implementation
 * across protocols and transports.
 * <p>
 * TServiceClient 用于跨协议和 Transport 的 TService 交互
 */
public abstract class TServiceClient {

    protected TProtocol iprot_;
    protected TProtocol oprot_;
    protected int seqid_;

    public TServiceClient(TProtocol prot) {
        this(prot, prot);
    }

    public TServiceClient(TProtocol iprot, TProtocol oprot) {
        iprot_ = iprot;
        oprot_ = oprot;
    }

    /**
     * Get the TProtocol being used as the input (read) protocol.
     *
     * @return the TProtocol being used as the input (read) protocol.
     */
    public TProtocol getInputProtocol() {
        return this.iprot_;
    }

    /**
     * Get the TProtocol being used as the output (write) protocol.
     *
     * @return the TProtocol being used as the output (write) protocol.
     */
    public TProtocol getOutputProtocol() {
        return this.oprot_;
    }

    /**
     * 基本调用
     *
     * @param methodName 方法名
     * @param args       请求参数
     * @throws TException
     */
    protected void sendBase(String methodName, TBase<?, ?> args) throws TException {
        sendBase(methodName, args, TMessageType.CALL);
    }

    /**
     * oneway 调用
     *
     * @param methodName 方法名
     * @param args       参数
     * @throws TException
     */
    protected void sendBaseOneway(String methodName, TBase<?, ?> args) throws TException {
        sendBase(methodName, args, TMessageType.ONEWAY);
    }

    /**
     * 发送调用
     *
     * @param methodName 方法名
     * @param args       请求参数
     * @param type       调用类型
     * @throws TException
     */
    private void sendBase(String methodName, TBase<?, ?> args, byte type) throws TException {
        // 构建请求，写入头信息
        oprot_.writeMessageBegin(new TMessage(methodName, type, ++seqid_));
        // 写入协议对象
        args.write(oprot_);
        // 写入结尾信息
        oprot_.writeMessageEnd();
        // 清空缓冲，写入
        oprot_.getTransport().flush();
    }

    /**
     * 获取消息
     *
     * @param result     返回的对象
     * @param methodName 方法名
     * @throws TException
     */
    protected void receiveBase(TBase<?, ?> result, String methodName) throws TException {
        // 读取消息
        TMessage msg = iprot_.readMessageBegin();
        // 如果是异常，则读取异常并抛出
        if (msg.type == TMessageType.EXCEPTION) {
            TApplicationException x = new TApplicationException();
            x.read(iprot_);
            iprot_.readMessageEnd();
            throw x;
        }
        // 如果请求序号不匹配，则抛出异常
        if (msg.seqid != seqid_) {
            throw new TApplicationException(TApplicationException.BAD_SEQUENCE_ID,
                    String.format("%s failed: out of sequence response: expected %d but got %d", methodName, seqid_, msg.seqid));
        }
        // 读取响应内容
        result.read(iprot_);
        iprot_.readMessageEnd();
    }
}
