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
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

/**
 * Servlet implementation class ThriftServer, that allows {@link TProcessor} and
 * {@link TProtocolFactory} to be supplied after the {@link #init()} method has
 * finished. <br>
 * Thrift 服务端 Servlet 的实现，支持在完成 init 方法之后提供 TProcessor 和 TProtocolFactory
 * <p>
 * Subclasses must implement the abstract methods that return the TProcessor and
 * two TProtocolFactory. Those methods are guaranteed to be called exactly once,
 * and that {@link ServletContext} is available.
 * 子类必须实现重新方法返回 TProcessor 和 TProtocolFactory，这些方法保证只调用一次，
 * 并且 ServletContext 是可用的
 */
public abstract class TExtensibleServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    private TProcessor processor;

    private TProtocolFactory inFactory;

    private TProtocolFactory outFactory;

    private Collection<Map.Entry<String, String>> customHeaders;

    /**
     * Returns the appropriate {@link TProcessor}. This will be called <b>once</b> just
     * after the {@link #init()} method
     * 返回适当的 TProcessor，在 init 之后只调用一次
     *
     * @return the appropriate {@link TProcessor}
     */
    protected abstract TProcessor getProcessor();

    /**
     * Returns the appropriate in {@link TProtocolFactory}. This will be called
     * <b>once</b> just after the {@link #init()} method
     * 返回适当的 TProtocolFactory，在 init 之后只调用一次
     *
     * @return the appropriate in {@link TProtocolFactory}
     */
    protected abstract TProtocolFactory getInProtocolFactory();

    /**
     * Returns the appropriate out {@link TProtocolFactory}. This will be called
     * <b>once</b> just after the {@link #init()} method
     * 返回适当的 TProtocolFactory，在 init 之后只调用一次
     *
     * @return the appropriate out {@link TProtocolFactory}
     */
    protected abstract TProtocolFactory getOutProtocolFactory();

    @Override
    public final void init(ServletConfig config) throws ServletException {
        super.init(config); //no-args init() happens here
        // 初始化相应的 Processor 和 Factory
        this.processor = getProcessor();
        this.inFactory = getInProtocolFactory();
        this.outFactory = getOutProtocolFactory();
        this.customHeaders = new ArrayList<Map.Entry<String, String>>();

        if (processor == null) {
            throw new ServletException("processor must be set");
        }
        if (inFactory == null) {
            throw new ServletException("inFactory must be set");
        }
        if (outFactory == null) {
            throw new ServletException("outFactory must be set");
        }
    }

    /**
     * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
     * response)
     */
    @Override
    protected void doPost(HttpServletRequest request,
                          HttpServletResponse response) throws ServletException, IOException {
        TTransport inTransport = null;
        TTransport outTransport = null;

        try {
            response.setContentType("application/x-thrift");

            // 如果有自定义的 header，则添加到响应中
            if (null != this.customHeaders) {
                for (Map.Entry<String, String> header : this.customHeaders) {
                    response.addHeader(header.getKey(), header.getValue());
                }
            }

            InputStream in = request.getInputStream();
            OutputStream out = response.getOutputStream();

            TTransport transport = new TIOStreamTransport(in, out);
            inTransport = transport;
            outTransport = transport;

            TProtocol inProtocol = inFactory.getProtocol(inTransport);
            TProtocol outProtocol = inFactory.getProtocol(outTransport);

            // 处理请求
            processor.process(inProtocol, outProtocol);
            out.flush();
        } catch (TException te) {
            throw new ServletException(te);
        }
    }

    /**
     * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
     * response)
     */
    @Override
    protected void doGet(HttpServletRequest req,
                         HttpServletResponse resp) throws ServletException, IOException {
        doPost(req, resp);
    }

    /**
     * 添加自定义的 Header
     */
    public void addCustomHeader(final String key, final String value) {
        this.customHeaders.add(new Map.Entry<String, String>() {
            public String getKey() {
                return key;
            }

            public String getValue() {
                return value;
            }

            public String setValue(String value) {
                return null;
            }
        });
    }

    /**
     * 设置自定义的 header
     */
    public void setCustomHeaders(Collection<Map.Entry<String, String>> headers) {
        this.customHeaders.clear();
        this.customHeaders.addAll(headers);
    }
}
