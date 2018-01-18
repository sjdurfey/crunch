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
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.avro.AvroObjectInspectorGenerator;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hive.hcatalog.data.HCatRecord;

import java.util.List;

/**
 * A root context for processing an {@link HCatRecord}
 */
public class HCatParseRootContext extends HCatParseContext {

  // fieldRefs for each element in the struct. this is used
  // to get object inspectors for each field
  private final List<? extends StructField> fieldRefs;
  // the data within the struct
  private final List<Object> fieldData;
  // the position within the fieldData current being examined
  private int index;
  // type information for each column in the struct
  private final List<TypeInfo> columnTypes;

  protected HCatParseRootContext(HCatRecord record, Schema tableSchema) {
    super(null, tableSchema);

    // creates the object inspector and type information
    // for each element within the writer schema. this is
    // the source of all hcat info for downstream processing
    AvroObjectInspectorGenerator aoig;
    try {
      aoig = new AvroObjectInspectorGenerator(tableSchema);
    } catch (SerDeException e) {
      throw new RuntimeException(e);
    }

    columnTypes = aoig.getColumnTypes();
    StructObjectInspector oi = (StructObjectInspector) aoig.getObjectInspector();
    fieldRefs = oi.getAllStructFieldRefs();
    fieldData = oi.getStructFieldsDataAsList(record.getAll());
    index = 0;
    size = remainingElements = fieldData.size();
  }

  @Override
  public boolean done() {
    return getRemainingSize() == 0;
  }

  @Override
  public Object nextValue() {
    if (done())
      throw new IllegalStateException("Another value was requested, but context is finished");

    currentFieldSchema = schema.getFields().get(index).schema();
    currentTypeInfo = columnTypes.get(index);
    StructField structFieldRef = fieldRefs.get(index);
    Object structFieldData = fieldData.get(index);
    currentValueObjectInspector = structFieldRef.getFieldObjectInspector();

    index++;
    remainingElements--;
    return structFieldData;
  }
}
