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

package org.apache.thrift.transport;

/**
 * Factory class used to create wrapped instance of Transports.
 * This is used primarily in servers, which get Transports from
 * a ServerTransport and then may want to mutate them (i.e. create
 * a BufferedTransport from the underlying base transport)
 * 用于创建 Transport 实例的工厂，主要用于 Server，从 ServerTransport 中
 * 获取 Transport，然后可能尝试变异，如创建 BufferedTransport
 */
public class TTransportFactory {

    /**
     * Return a wrapped instance of the base Transport.
     * 返回使用基本的 Transport 封装之后的实例
     *
     * @param trans The base transport
     *              基础的 Transport
     * @return Wrapped Transport
     * 封装之后的 Transport
     */
    public TTransport getTransport(TTransport trans) {
        return trans;
    }

}
