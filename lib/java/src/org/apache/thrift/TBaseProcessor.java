package org.apache.thrift;

import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TType;

import java.util.Collections;
import java.util.Map;

/**
 * 是一个用于操作输入和输出流的处理器
 */
public abstract class TBaseProcessor<I> implements TProcessor {

    // 要处理的接口
    private final I iface;

    // 处理对象集合
    private final Map<String, ProcessFunction<I, ? extends TBase>> processMap;

    // 构建处理器
    protected TBaseProcessor(I iface, Map<String, ProcessFunction<I, ? extends TBase>> processFunctionMap) {
        this.iface = iface;
        this.processMap = processFunctionMap;
    }

    public Map<String, ProcessFunction<I, ? extends TBase>> getProcessMapView() {
        return Collections.unmodifiableMap(processMap);
    }

    @Override
    public void process(TProtocol in, TProtocol out) throws TException {
        // 读取 Message
        TMessage msg = in.readMessageBegin();

        // 根据名称获取处理的方法
        ProcessFunction fn = processMap.get(msg.name);

        // 如果没有处理方法，则返回无效
        if (fn == null) {
            TProtocolUtil.skip(in, TType.STRUCT);
            in.readMessageEnd();
            // 生成异常，写入到输出流中
            TApplicationException x = new TApplicationException(TApplicationException.UNKNOWN_METHOD, "Invalid method name: '" + msg.name + "'");
            out.writeMessageBegin(new TMessage(msg.name, TMessageType.EXCEPTION, msg.seqid));
            x.write(out);
            out.writeMessageEnd();
            out.getTransport().flush();
        } else {
            // 使用处理器进行处理
            fn.process(msg.seqid, in, out, iface);
        }
    }
}
