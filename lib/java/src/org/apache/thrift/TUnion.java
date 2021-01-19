/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.thrift;

import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;
import org.apache.thrift.scheme.TupleScheme;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Union 结构体
 *
 * @param <T>
 * @param <F>
 */
public abstract class TUnion<T extends TUnion<T, F>, F extends TFieldIdEnum> implements TBase<T, F> {

    /**
     * 协议集合
     */
    private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();

    static {
        schemes.put(StandardScheme.class, new TUnionStandardSchemeFactory());
        schemes.put(TupleScheme.class, new TUnionTupleSchemeFactory());
    }

    protected Object value_;
    protected F setField_;

    protected TUnion() {
        setField_ = null;
        value_ = null;
    }

    protected TUnion(F setField, Object value) {
        setFieldValue(setField, value);
    }

    protected TUnion(TUnion<T, F> other) {
        if (!other.getClass().equals(this.getClass())) {
            throw new ClassCastException();
        }
        setField_ = other.setField_;
        value_ = deepCopyObject(other.value_);
    }

    /**
     * 深拷贝对象
     *
     * @param o 对象
     * @return 拷贝的值
     */
    private static Object deepCopyObject(Object o) {
        if (o instanceof TBase) {
            return ((TBase) o).deepCopy();
        } else if (o instanceof ByteBuffer) {
            return TBaseHelper.copyBinary((ByteBuffer) o);
        } else if (o instanceof List) {
            return deepCopyList((List) o);
        } else if (o instanceof Set) {
            return deepCopySet((Set) o);
        } else if (o instanceof Map) {
            return deepCopyMap((Map) o);
        } else {
            return o;
        }
    }

    private static Map deepCopyMap(Map<Object, Object> map) {
        Map copy = new HashMap(map.size());
        for (Map.Entry<Object, Object> entry : map.entrySet()) {
            copy.put(deepCopyObject(entry.getKey()), deepCopyObject(entry.getValue()));
        }
        return copy;
    }

    private static Set deepCopySet(Set set) {
        Set copy = new HashSet(set.size());
        for (Object o : set) {
            copy.add(deepCopyObject(o));
        }
        return copy;
    }

    private static List deepCopyList(List list) {
        List copy = new ArrayList(list.size());
        for (Object o : list) {
            copy.add(deepCopyObject(o));
        }
        return copy;
    }

    public F getSetField() {
        return setField_;
    }

    public Object getFieldValue() {
        return value_;
    }

    public Object getFieldValue(F fieldId) {
        if (fieldId != setField_) {
            throw new IllegalArgumentException("Cannot get the value of field " + fieldId + " because union's set field is " + setField_);
        }

        return getFieldValue();
    }

    public Object getFieldValue(int fieldId) {
        return getFieldValue(enumForId((short) fieldId));
    }

    public boolean isSet() {
        return setField_ != null;
    }

    public boolean isSet(F fieldId) {
        return setField_ == fieldId;
    }

    public boolean isSet(int fieldId) {
        return isSet(enumForId((short) fieldId));
    }

    /**
     * 读取
     *
     * @param iprot Input protocol
     * @throws TException
     */
    public void read(TProtocol iprot) throws TException {
        schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
    }

    /**
     * 设置属性和值
     *
     * @param fieldId 属性 ID
     * @param value   值
     */
    public void setFieldValue(F fieldId, Object value) {
        checkType(fieldId, value);
        setField_ = fieldId;
        value_ = value;
    }

    public void setFieldValue(int fieldId, Object value) {
        setFieldValue(enumForId((short) fieldId), value);
    }

    /**
     * 消息输出到协议
     *
     * @param oprot Output protocol
     *              协议
     * @throws TException
     */
    public void write(TProtocol oprot) throws TException {
        schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
    }

    /**
     * Implementation should be generated so that we can efficiently type check
     * various values.
     *
     * @param setField
     * @param value
     */
    protected abstract void checkType(F setField, Object value) throws ClassCastException;

    /**
     * Implementation should be generated to read the right stuff from the wire
     * based on the field header.
     * <p>
     * 实现应当基于属性头线性读取正确的内容
     *
     * @param field 属性
     * @return read Object based on the field header, as specified by the argument.
     * 基于属性头读取的对象，和参数指定的一致
     */
    protected abstract Object standardSchemeReadValue(TProtocol iprot, TField field) throws TException;

    protected abstract void standardSchemeWriteValue(TProtocol oprot) throws TException;

    protected abstract Object tupleSchemeReadValue(TProtocol iprot, short fieldID) throws TException;

    protected abstract void tupleSchemeWriteValue(TProtocol oprot) throws TException;

    protected abstract TStruct getStructDesc();

    protected abstract TField getFieldDesc(F setField);

    protected abstract F enumForId(short id);

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("<");
        sb.append(this.getClass().getSimpleName());
        sb.append(" ");

        if (getSetField() != null) {
            Object v = getFieldValue();
            sb.append(getFieldDesc(getSetField()).name);
            sb.append(":");
            if (v instanceof ByteBuffer) {
                TBaseHelper.toString((ByteBuffer) v, sb);
            } else {
                sb.append(v.toString());
            }
        }
        sb.append(">");
        return sb.toString();
    }

    public final void clear() {
        this.setField_ = null;
        this.value_ = null;
    }

    /**
     * Union 标准协议工厂
     */
    private static class TUnionStandardSchemeFactory implements SchemeFactory {
        public TUnionStandardScheme getScheme() {
            return new TUnionStandardScheme();
        }
    }

    /**
     * Union 标准协议
     */
    private static class TUnionStandardScheme extends StandardScheme<TUnion> {

        /**
         * 读取内容到结构体中
         *
         * @param iprot  协议
         * @param struct 结构体
         * @throws TException
         */
        @Override
        public void read(TProtocol iprot, TUnion struct) throws TException {
            struct.setField_ = null;
            struct.value_ = null;

            iprot.readStructBegin();

            TField field = iprot.readFieldBegin();

            // 读取值
            struct.value_ = struct.standardSchemeReadValue(iprot, field);

            if (struct.value_ != null) {
                struct.setField_ = struct.enumForId(field.id);
            }

            iprot.readFieldEnd();
            // this is so that we will eat the stop byte. we could put a check here to
            // make sure that it actually *is* the stop byte, but it's faster to do it
            // this way.
            iprot.readFieldBegin();
            iprot.readStructEnd();
        }

        /**
         * 将对象写入到协议中
         *
         * @param oprot  协议
         * @param struct 结构体
         * @throws TException
         */
        @Override
        public void write(TProtocol oprot, TUnion struct) throws TException {
            if (struct.getSetField() == null || struct.getFieldValue() == null) {
                throw new TProtocolException("Cannot write a TUnion with no set value!");
            }

            oprot.writeStructBegin(struct.getStructDesc());
            oprot.writeFieldBegin(struct.getFieldDesc(struct.setField_));
            struct.standardSchemeWriteValue(oprot);
            oprot.writeFieldEnd();
            oprot.writeFieldStop();
            oprot.writeStructEnd();
        }
    }

    /**
     * Union 协议工厂
     */
    private static class TUnionTupleSchemeFactory implements SchemeFactory {
        public TUnionTupleScheme getScheme() {
            return new TUnionTupleScheme();
        }
    }

    /**
     * Union Tuple 协议
     */
    private static class TUnionTupleScheme extends TupleScheme<TUnion> {

        /**
         * 读取
         *
         * @param iprot
         * @param struct 结构体
         * @throws TException
         */
        @Override
        public void read(TProtocol iprot, TUnion struct) throws TException {
            struct.setField_ = null;
            struct.value_ = null;
            short fieldID = iprot.readI16();
            struct.value_ = struct.tupleSchemeReadValue(iprot, fieldID);
            if (struct.value_ != null) {
                struct.setField_ = struct.enumForId(fieldID);
            }
        }

        /**
         * 写入
         *
         * @param oprot
         * @param struct 结构体
         * @throws TException
         */
        @Override
        public void write(TProtocol oprot, TUnion struct) throws TException {
            if (struct.getSetField() == null || struct.getFieldValue() == null) {
                throw new TProtocolException("Cannot write a TUnion with no set value!");
            }
            oprot.writeI16(struct.setField_.getThriftFieldId());
            struct.tupleSchemeWriteValue(oprot);
        }
    }
}
