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
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hive.hcatalog.data.HCatRecord;

/**
 * A context to describe a particular node in the parsing graph, and for
 * creating additional nodes within the graph. Also provides common methods to
 * use to assess how to treat the node, and retrieve HCatalog specific
 * information needed to retrieve/read the underlying data, as well as helper
 * methods for creating new nodes based upon the node type.
 */
public abstract class HCatParseContext {

  // pointer to the parent of the node
  private final HCatParseContext parent;
  // the avro schema for the current node
  protected final Schema schema;

  // the number of elements left to process
  protected long remainingElements;
  // the number of elements in the context to process
  protected long size;

  // type of the current value being examined
  protected TypeInfo currentTypeInfo;
  // avro schema for the current element in the struct
  protected Schema currentFieldSchema;
  // object inspector for the current element
  protected ObjectInspector currentValueObjectInspector;

  boolean skipping = false;

  protected HCatParseContext(HCatParseContext parent, Schema schema) {
    this.parent = parent;
    this.schema = schema;
  }

  /**
   * Marks the contents of this context to be skipped
   */
  public void startSkipping() {
    skipping = true;
  }

  /**
   * Unmarks the contents of this context for skipping
   */
  public void stopSkipping() {
    skipping = false;
  }

  /**
   * Returns whether or not the current context is being skipped. This flag is
   * necessary for complex types like a list or a map.
   *
   * @return true if skipping the value, false if not
   */
  public boolean skipping() {
    return skipping;
  }

  /**
   * Returns the schema for the current value in the node
   * 
   * @return schema for the current value in the node
   */
  public Schema getValueSchema() {
    return currentFieldSchema;
  }

  /**
   * The hcat {@link TypeInfo} for the current value in the node
   * 
   * @return hive type info for the current value
   */
  public TypeInfo getValueTypeInfo() {
    return currentTypeInfo;
  }

  /**
   * The hcat {@link ObjectInspector} for the current value
   * 
   * @return the hive object inspector
   */
  public ObjectInspector getValueObjectInspector() {
    return currentValueObjectInspector;
  }

  /**
   * Indicates whether or not this context has finished reading all data within
   * the node.
   * 
   * @return true if context is done processing
   */
  public abstract boolean done();

  /**
   * Returns the next value within the node
   * 
   * @return the next value in the context
   */
  public abstract Object nextValue();

  /**
   * Returns the number of elements within this context
   * 
   * @return the number of elements in the context
   */
  public long getSize() {
    return size;
  }

  /**
   * Returns the number of elements left to process within this context
   * 
   * @return the number of elements left in the context
   */
  public long getRemainingSize() {
    return remainingElements;
  }

  /**
   * Indicates whether or not data has been read from this context
   * 
   * @return true if processing has started, false otherwise
   */
  public boolean started() {
    // processing of the context hasn't started, unless remaining != size
    // or the sizes are the same and the context is being skipped
    return getSize() != getRemainingSize();
  }

  /**
   * The parent of this context
   * 
   * @return the parent context
   */
  public HCatParseContext getParent() {
    return parent;
  }

  /**
   * The schema for the node in this context
   * 
   * @return the context schema
   */
  public Schema getSchema() {
    return schema;
  }

  /**
   * Creates a root context. This context should be the root of the graph (e.g.
   * the starting point)
   * 
   * @param record
   *          the record to start traversing
   * @param tableSchema
   *          the schema used when creating the record
   * @return a context to start processing the root
   */
  public static HCatParseContext createRootContext(HCatRecord record, Schema tableSchema) {
    return new HCatParseRootContext(record, tableSchema);
  }

  /**
   * Create a map context
   *
   * @param mapObj
   *          the map object to process
   * @param typeInfo
   *          the type info for the map
   * @param inspector
   *          the object inspector for the map
   * @param schema
   *          the avro schema for the map
   *
   * @return
   */
  public HCatParseContext createChildMapContext(Object mapObj, TypeInfo typeInfo, ObjectInspector inspector,
      Schema schema) {
    return new HCatParseMapContext(this, mapObj, typeInfo, inspector, schema);
  }

  /**
   * Create a list context
   *
   * @param listObj
   *          the list object to process
   * @param typeInfo
   *          the type info for the list
   * @param inspector
   *          the object inspector for the list
   * @param schema
   *          the avro schema for the list
   *
   * @return
   */
  public HCatParseContext createChildListContext(Object listObj, TypeInfo typeInfo, ObjectInspector inspector,
      Schema schema) {
    return new HCatParseListContext(this, listObj, typeInfo, inspector, schema);
  }

  /**
   * Create a struct context
   *
   * @param structObj
   *          the struct object to process
   * @param typeInfo
   *          the type info for the struct
   * @param inspector
   *          the object inspector for the struct
   * @param schema
   *          the avro schema for the struct
   *
   * @return
   */
  public HCatParseContext createChildStructContext(Object structObj, TypeInfo typeInfo, ObjectInspector inspector,
      Schema schema) {
    return new HCatParseStructContext(this, structObj, typeInfo, inspector, schema);
  }

  /**
   * Create a union context. Can be used for Hive unions, or Avro unions
   *
   * @param unionObj
   *          the union object to process
   * @param typeInfo
   *          the type info for the union
   * @param inspector
   *          the object inspector for the union
   * @param schema
   *          the avro schema for the union
   *
   * @return
   */
  public HCatParseContext createChildUnionContext(Object unionObj, TypeInfo typeInfo, ObjectInspector inspector,
      Schema schema) {
    return new HCatParseUnionContext(this, unionObj, typeInfo, inspector, schema);
  }
}
