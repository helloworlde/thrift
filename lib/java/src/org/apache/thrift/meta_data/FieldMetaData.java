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

package org.apache.thrift.meta_data;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;

import java.util.HashMap;
import java.util.Map;

/**
 * This class is used to store meta data about thrift fields. Every field in a
 * a struct should have a corresponding instance of this class describing it.
 * <p>
 * 这个类用于存储 Thrift 属性的元数据，每一个属性都应当有对应的的实例用于描述这个类
 */
public class FieldMetaData implements java.io.Serializable {
    // 结构体集合
    private static Map<Class<? extends TBase>, Map<? extends TFieldIdEnum, FieldMetaData>> structMap;

    static {
        structMap = new HashMap<Class<? extends TBase>, Map<? extends TFieldIdEnum, FieldMetaData>>();
    }

    // 属性名称
    public final String fieldName;
    // 必需类型
    public final byte requirementType;
    // 值的元数据
    public final FieldValueMetaData valueMetaData;

    public FieldMetaData(String name, byte req, FieldValueMetaData vMetaData) {
        this.fieldName = name;
        this.requirementType = req;
        this.valueMetaData = vMetaData;
    }

    /**
     * 添加到集合中
     *
     * @param sClass 类
     * @param map    结构体集合
     */
    public static synchronized void addStructMetaDataMap(Class<? extends TBase> sClass,
                                                         Map<? extends TFieldIdEnum, FieldMetaData> map) {
        structMap.put(sClass, map);
    }

    /**
     * Returns a map with metadata (i.e. instances of FieldMetaData) that
     * describe the fields of the given class.
     * <p>
     * 使用元数据类型获取描述属性的元数据
     *
     * @param sClass The TBase class for which the metadata map is requested
     */
    public static synchronized Map<? extends TFieldIdEnum, FieldMetaData> getStructMetaDataMap(Class<? extends TBase> sClass) {
        if (!structMap.containsKey(sClass)) { // Load class if it hasn't been loaded
            try {
                sClass.newInstance();
            } catch (InstantiationException e) {
                throw new RuntimeException("InstantiationException for TBase class: " + sClass.getName() + ", message: " + e.getMessage());
            } catch (IllegalAccessException e) {
                throw new RuntimeException("IllegalAccessException for TBase class: " + sClass.getName() + ", message: " + e.getMessage());
            }
        }
        return structMap.get(sClass);
    }
}
