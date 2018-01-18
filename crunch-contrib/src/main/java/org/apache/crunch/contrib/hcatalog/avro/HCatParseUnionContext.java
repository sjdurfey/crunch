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
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * A context for parsing an hcat union or an avro union
 */
public class HCatParseUnionContext extends HCatParseContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(HCatParseUnionContext.class);

  private final Object value;
  private int tag;
  private boolean readValue = false;

  public HCatParseUnionContext(HCatParseContext parent, Object value, TypeInfo typeInfo, ObjectInspector inspector,
      Schema schema) {
    super(parent, schema);

    size = remainingElements = 0;
    // class is used for Hive Unions and Avro Unions
    if (inspector instanceof UnionObjectInspector) {
      LOGGER.debug("In UnionContext for HCat Union");
      UnionObjectInspector objectInspector = (UnionObjectInspector) inspector;
      tag = objectInspector.getTag(value);

      if (LOGGER.isDebugEnabled())
        LOGGER.debug("Found tag for hcat union [{}]", tag);

      // Invariant that Avro's tag ordering must match Hive's.
      UnionTypeInfo info = (UnionTypeInfo) typeInfo;
      currentTypeInfo = info.getAllUnionObjectTypeInfos().get(tag);
      currentValueObjectInspector = objectInspector.getObjectInspectors().get(tag);
      currentFieldSchema = schema.getTypes().get(tag);
      this.value = objectInspector.getField(value);
    } else { // union type is an avro union
      LOGGER.debug("In UnionContext for Avro Union");
      currentValueObjectInspector = inspector;
      this.value = value;
      currentTypeInfo = typeInfo;

      List<Schema> valueTypes = schema.getTypes();

      tag = -1;
      for (int i = 0; i < valueTypes.size(); i++) {
        // for unions that have complex types the category name needs to be
        // inspected (e.g. a map will have a category name of MAP, but a type
        // name that is equal to the schema). the value within valueTypes will
        // have a type that is equal to what it is (e.g. map, string, int,
        // union, array, etc)
        String typeName = currentTypeInfo.getTypeName();
        if (currentTypeInfo.getCategory() != ObjectInspector.Category.PRIMITIVE) {
          typeName = currentTypeInfo.getCategory().name();
        }

        if (valueTypes.get(i).getType().toString().equalsIgnoreCase(typeName)) {
          tag = i;
          break;
        }
      }

      if (LOGGER.isDebugEnabled()) {
        String typeName = currentTypeInfo.getTypeName();
        if (tag == -1)
          LOGGER.debug("Unable to find tag for type [{}]", typeName);
        else
          LOGGER.debug("Found tag [{}] for type [{}]", tag, typeName);
      }

      currentFieldSchema = valueTypes.get(tag);
    }
  }

  @Override
  public boolean done() {
    return readValue;
  }

  @Override
  public boolean started() {
    return readValue;
  }

  @Override
  public Object nextValue() {
    LOGGER.debug("Value from union has been read");
    readValue = true;
    return value;
  }

  /**
   * Identifies the index within the list of possible types for this union. For
   * example, if a union has the following possible types:
   * 
   * <pre>
   *     ['string', 'null', 'int']
   * </pre>
   * 
   * And the value in the union is of type 'string', then this method will
   * return 0. If it is an integer, then it will return 2
   * 
   * @return
   */
  public int getTag() {
    return tag;
  }
}
