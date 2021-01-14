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

import java.io.Serializable;

/**
 * Generic base interface for generated Thrift objects.
 * Thrift 对象的泛化接口
 */
public interface TBase<T extends TBase<T, F>, F extends TFieldIdEnum> extends Comparable<T>, TSerializable, Serializable {

    /**
     * Get the F instance that corresponds to fieldId.
     * 根据 fieldId 获取实例
     */
    public F fieldForId(int fieldId);

    /**
     * Check if a field is currently set or unset.
     * 检查当前的属性是否设置
     *
     * @param field
     */
    public boolean isSet(F field);

    /**
     * Get a field's value by field variable. Primitive types will be wrapped in
     * the appropriate "boxed" types.
     * 根据属性变量获取属性值，基本类型会被自动装箱为复合类型
     *
     * @param field
     */
    public Object getFieldValue(F field);

    /**
     * Set a field's value by field variable. Primitive types must be "boxed" in
     * the appropriate object wrapper type.
     * 为属性设置值，必须是封装后的复合类型
     *
     * @param field
     */
    public void setFieldValue(F field, Object value);

    /**
     * 拷贝对象
     */
    public T deepCopy();

    /**
     * Return to the state of having just been initialized, as though you had just
     * called the default constructor.
     * 返回初始化状态的实例，仅需要调用默认构造方法
     */
    public void clear();
}
