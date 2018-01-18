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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.Utf8;
import org.apache.crunch.contrib.hcatalog.avro.HCatReaderWriterFactory;

import java.io.IOException;

public class TestHCatReaderWriterFactory implements HCatReaderWriterFactory {

  public class TestSpecificDatumReader extends SpecificDatumReader {

    public TestSpecificDatumReader(Schema reader, Schema writer) {
      super(reader, writer);
    }

    @Override
    protected Object readString(Object old, Schema expected, Decoder in) throws IOException {
      in.readString(old instanceof Utf8 ? (Utf8)old : null);
      return "overridden string from test";
    }
  }

  @Override
  public GenericData getData() {
    return SpecificData.get();
  }

  @Override
  public <D> DatumReader<D> getReader(Schema readerSchema, Schema writerSchema) {
    return new TestSpecificDatumReader(readerSchema, writerSchema);
  }

  @Override
  public <D> DatumWriter<D> getWriter(Schema schema) {
    throw new UnsupportedOperationException("Does not support converting to HCatRecord from Avro");
  }
}
