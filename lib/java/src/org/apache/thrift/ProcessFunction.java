package org.apache.thrift;

import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 逻辑处理器
 */
public abstract class ProcessFunction<I, T extends TBase> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessFunction.class.getName());
    // 方法名称
    private final String methodName;

    /**
     * 使用方法名称构造处理器
     *
     * @param methodName 方法名称
     */
    public ProcessFunction(String methodName) {
        this.methodName = methodName;
    }

    /**
     * 处理方法调用
     *
     * @param seqid 序号
     * @param iprot
     * @param oprot
     * @param iface 定义接口
     */
    public final void process(int seqid,
                              TProtocol iprot,
                              TProtocol oprot,
                              I iface) throws TException {

        // 获取空参数实例
        T args = getEmptyArgsInstance();
        try {
            // 读取
            args.read(iprot);
        } catch (TProtocolException e) {
            // 如果失败则写入异常，返回失败
            iprot.readMessageEnd();
            TApplicationException x = new TApplicationException(TApplicationException.PROTOCOL_ERROR, e.getMessage());
            oprot.writeMessageBegin(new TMessage(getMethodName(), TMessageType.EXCEPTION, seqid));
            x.write(oprot);
            oprot.writeMessageEnd();
            oprot.getTransport().flush();
            return;
        }
        iprot.readMessageEnd();
        TSerializable result = null;
        byte msgType = TMessageType.REPLY;

        try {
            // 获取结果
            result = getResult(iface, args);
        } catch (TTransportException ex) {
            LOGGER.error("Transport error while processing " + getMethodName(), ex);
            throw ex;
        } catch (TApplicationException ex) {
            LOGGER.error("Internal application error processing " + getMethodName(), ex);
            result = ex;
            msgType = TMessageType.EXCEPTION;
        } catch (Exception ex) {
            LOGGER.error("Internal error processing " + getMethodName(), ex);
            if (rethrowUnhandledExceptions()) throw new RuntimeException(ex.getMessage(), ex);
            if (!isOneway()) {
                result = new TApplicationException(TApplicationException.INTERNAL_ERROR,
                        "Internal error processing " + getMethodName());
                msgType = TMessageType.EXCEPTION;
            }
        }

        // 如果不是 oneway 的，则写入响应结果
        if (!isOneway()) {
            oprot.writeMessageBegin(new TMessage(getMethodName(), msgType, seqid));
            result.write(oprot);
            oprot.writeMessageEnd();
            oprot.getTransport().flush();
        }
    }

    private void handleException(int seqid, TProtocol oprot) throws TException {
        if (!isOneway()) {
            TApplicationException x = new TApplicationException(TApplicationException.INTERNAL_ERROR,
                    "Internal error processing " + getMethodName());
            oprot.writeMessageBegin(new TMessage(getMethodName(), TMessageType.EXCEPTION, seqid));
            x.write(oprot);
            oprot.writeMessageEnd();
            oprot.getTransport().flush();
        }
    }

    protected boolean rethrowUnhandledExceptions() {
        return false;
    }

    protected abstract boolean isOneway();

    /**
     * 获取响应结果
     *
     * @param iface 接口
     * @param args  参数
     * @return 结果
     */
    public abstract TBase getResult(I iface, T args) throws TException;

    /**
     * 返回空的参数实例
     */
    public abstract T getEmptyArgsInstance();

    public String getMethodName() {
        return methodName;
    }
}
