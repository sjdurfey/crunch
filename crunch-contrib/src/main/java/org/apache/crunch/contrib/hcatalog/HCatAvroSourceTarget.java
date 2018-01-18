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
package org.apache.crunch.contrib.hcatalog;

import org.apache.crunch.io.hcatalog.HCatSourceTarget;
import org.apache.hive.hcatalog.data.HCatRecord;

import javax.annotation.Nullable;

/**
 * An HCatSourceTarget used to configure the source for converting an HCatRecord into a specific
 * avro record.
 */
public class HCatAvroSourceTarget extends HCatSourceTarget {

  /** The configuration property for setting the avro schema from the source hive table */
  public static final String AVRO_TABLE_SCHEMA_PROP = "avro.serde.schema";

  /**
   * The configuration property for setting a custom {@link
   * org.apache.crunch.contrib.hcatalog.avro.HCatReaderWriterFactory} when converting {@link HCatRecord}'s to avro
   */
  public static final String DATUM_READER_WRITER_FACTORY_PROP = "crunch.avro.factory";

  public HCatAvroSourceTarget(String table) {
    super(table);
  }

  public HCatAvroSourceTarget(String database, String table) {
    super(database, table);
  }

  public HCatAvroSourceTarget(@Nullable String database, String table, String filter) {
    super(database, table, filter);
  }
}
