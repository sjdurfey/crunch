/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.crunch.contrib.hcatalog.avro;

import org.apache.avro.Schema;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * A context for parsing a hive struct
 */
public class HCatParseStructContext extends HCatParseContext {

  private final StructTypeInfo typeInfo;
  private final StructObjectInspector inspector;
  private final List<Object> fieldList;
  private int index = 0;
  private final List<? extends StructField> fieldRefs;
  private final ArrayList<TypeInfo> fieldInfos;

  public HCatParseStructContext(HCatParseContext parent, Object value, TypeInfo typeInfo, ObjectInspector inspector,
      Schema schema) {
    super(parent, schema);
    this.typeInfo = (StructTypeInfo) typeInfo;
    this.inspector = (StructObjectInspector) inspector;
    this.fieldRefs = this.inspector.getAllStructFieldRefs();
    this.fieldList = this.inspector.getStructFieldsDataAsList(value);
    this.fieldInfos = this.typeInfo.getAllStructFieldTypeInfos();
    size = remainingElements = fieldList.size();
  }

  @Override
  public boolean done() {
    return index >= fieldList.size();
  }

  @Override
  public Object nextValue() {
    if (done())
      throw new IllegalStateException("Another value was requested, but context is finished");

    Schema.Field field = schema.getFields().get(index);
    currentFieldSchema = field.schema();
    currentTypeInfo = fieldInfos.get(index);
    StructField structFieldRef = fieldRefs.get(index);
    currentValueObjectInspector = structFieldRef.getFieldObjectInspector();

    Object obj = fieldList.get(index);
    index++;
    remainingElements--;
    return obj;
  }
}
