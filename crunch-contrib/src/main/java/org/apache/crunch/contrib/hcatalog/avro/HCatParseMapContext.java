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
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

/**
 * A context for processing a map
 */
public class HCatParseMapContext extends HCatParseContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(HCatParseMapContext.class);

  // maintains the entry position within the map
  private final Iterator<? extends Map.Entry<?, ?>> entryIterator;
  // the map data
  private final Map<?, ?> map;
  // the object inspector for the key. should always be of type 'string'
  // as avro and have only allow map keys to be strings
  private final PrimitiveObjectInspector mapKeyObjectInspector;
  // the type info for the map key
  private final TypeInfo mapKeyTypeInfo;
  // tracks the current map entry returned by the iterator
  private Map.Entry currentEntry;
  // tracks whether or not the key or value have been read
  // we dont want to advance the iterator until the map
  // value has been read
  private boolean readKey;
  private boolean readValue;
  // keeps track of the number of keys to read
  // (initially will be the same size and the map size)
  // this is used to track if map reading has started,
  // but the first entry hasn't been read
  private long keysRemaining;

  public HCatParseMapContext(HCatParseContext parent, Object mapObj, TypeInfo typeInfo, ObjectInspector inspector,
      Schema schema) {
    super(parent, schema);

    MapObjectInspector mapObjectInspector = (MapObjectInspector) inspector;
    if (!verifyMapKey(mapObjectInspector.getMapKeyObjectInspector())) {
      throw new IllegalStateException("Requested map, but key is not of type string");
    }

    MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
    mapKeyObjectInspector = (PrimitiveObjectInspector) mapObjectInspector.getMapKeyObjectInspector();
    currentValueObjectInspector = mapObjectInspector.getMapValueObjectInspector();
    mapKeyTypeInfo = mapTypeInfo.getMapKeyTypeInfo();
    currentTypeInfo = mapTypeInfo.getMapValueTypeInfo();
    map = mapObjectInspector.getMap(mapObj);
    entryIterator = map.entrySet().iterator();
    currentFieldSchema = schema.getValueType();
    keysRemaining = remainingElements = size = map.size();
  }

  @Override
  // Avro calls for one value at a time, and will first ask for the map key,
  // followed by asking for the map value. therefore, the map.entry being looked
  // at cannot be changed until both the key and value have been read from the
  // entry.
  public Object nextValue() {
    if (done())
      throw new IllegalStateException("Another value was requested, but context is finished");

    // check to see if the current Map.Entry is finished (e.g. key & value have
    // been read)
    // if so, reset markers in preparation for the next entry from the iterator
    if (entryFinished())
      reset();

    // if the key hasn't been read, mark it as read, and return the key
    if (!readKey) {
      LOGGER.debug("Reading in the map key");
      readKey = true;
      currentEntry = entryIterator.next();
      keysRemaining--;
      return currentEntry.getKey();
    }

    LOGGER.debug("Reading in the map value");
    readValue = true;
    remainingElements--;
    return currentEntry.getValue();
  }

  @Override
  public boolean started() {
    // if the keys remaining is the same as the size, it means
    // that loadNextValue hasn't been called yet
    return keysRemaining != getSize();
  }

  @Override
  public boolean done() {
    // the map is finished when the iterator doesn't have anything left
    // AND the last retrieved map entry has been read completely
    return readValue && !entryIterator.hasNext();
  }

  /**
   * Returns the object inspector for the key
   *
   * @return
   */
  public PrimitiveObjectInspector getMapKeyObjectInspector() {
    return mapKeyObjectInspector;
  }

  /**
   * Returns the type info for the key
   *
   * @return
   */
  public TypeInfo getMapKeyTypeInfo() {
    return mapKeyTypeInfo;
  }

  private static boolean verifyMapKey(ObjectInspector mapKeyObjectInspector) {
    return mapKeyObjectInspector instanceof PrimitiveObjectInspector
        && ((PrimitiveObjectInspector) mapKeyObjectInspector).getPrimitiveCategory()
            .equals(PrimitiveObjectInspector.PrimitiveCategory.STRING);
  }

  // loadNextValue returns one part of the map entry at a time. this is used to
  // check
  // if both the key and value have been read before advancing
  private boolean entryFinished() {
    return readKey && readValue;
  }

  // resets boolean indicators. used once entryFinished() is true
  private void reset() {
    readKey = false;
    readValue = false;
  }
}
