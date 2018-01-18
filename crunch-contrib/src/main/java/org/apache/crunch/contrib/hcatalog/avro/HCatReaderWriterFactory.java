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
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

/**
 * A factory for providing avro {@link DatumReader}'s and {@link DatumWriter}'s
 * needed for serialization/conversion of
 * {@link org.apache.hive.hcatalog.data.HCatRecord}'s
 */
public interface HCatReaderWriterFactory {

  /**
   * Returns an instance of or subtype of {@link GenericData}
   * 
   * @return Returns an instance of or subtype of {@link GenericData}
   */
  GenericData getData();

  /**
   * Returns an instance of a {@link DatumReader} that uses the provided
   * {@code readerSchema} and {@code writerSchema}.
   * 
   * @param readerSchema
   *          the reader schema for the reader
   * @param writerSchema
   *          the writer schema for the reader
   * @param <D>
   *          the avro type of data to read
   * @return a {@link DatumReader} instance
   */
  <D> DatumReader<D> getReader(Schema readerSchema, Schema writerSchema);

  /**
   * Returns an instance of a {@link DatumWriter} that uses the provided
   * {@code schema} to write with.
   * 
   * @param schema
   *          the schema to use for writing
   * @param <D>
   *          the type of avro data to write
   * @return a {@link DatumWriter} instance
   */
  <D> DatumWriter<D> getWriter(Schema schema);
}
