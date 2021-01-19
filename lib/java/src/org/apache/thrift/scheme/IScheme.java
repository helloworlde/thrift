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
package org.apache.thrift.scheme;

import org.apache.thrift.TBase;

/**
 * 协议
 *
 * @param <T>
 */
public interface IScheme<T extends TBase> {

    /**
     * 从 Protocol 读取内容写入到结构体中
     *
     * @param iproto 输入协议
     * @param struct 结构体
     * @throws org.apache.thrift.TException
     */
    public void read(org.apache.thrift.protocol.TProtocol iproto, T struct) throws org.apache.thrift.TException;

    /**
     * 将结构体对象写入到 Protocol 中
     *
     * @param oproto 输出协议
     * @param struct 结构体
     * @throws org.apache.thrift.TException
     */
    public void write(org.apache.thrift.protocol.TProtocol oproto, T struct) throws org.apache.thrift.TException;

}
