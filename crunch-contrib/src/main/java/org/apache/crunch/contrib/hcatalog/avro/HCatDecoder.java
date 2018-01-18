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
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;
import org.apache.hive.hcatalog.data.HCatRecord;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;

/**
 * An avro {@link Decoder} used for converting an {@link HCatRecord} and into an
 * avro record (specific or generic).
 * <p>
 * For specific records:
 * 
 * <pre>
 * SpecificDatumReader<MySpecificRecord> reader = new SpecificDatumReader<>(writerSchema, readerSchema);
 * HCatDecoder decoder = new HCatDecoder(hCatRecord, writerSchema);
 * MySpecificRecord record = reader.read(null, decoder);
 * </pre>
 * 
 * For generic records:
 * 
 * <pre>
 * GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(writerSchema, readerSchema);
 * HCatDecoder decoder = new HCatDecoder(hCatRecord, writerSchema);
 * GenericRecord record = reader.read(null, decoder);
 * </pre>
 * </p>
 */
public class HCatDecoder extends Decoder {

  private final HCatParser hCatParser;

  /**
   * Creates a new instance to read data from the provided {@code record}
   * 
   * @param record
   *          the record to read
   * @param tableSchema
   *          the avro schema describing the {@code record}
   *
   * @throws IllegalArgumentException
   *           if {@code record} or {@code tableSchema} are null
   */
  public HCatDecoder(HCatRecord record, Schema tableSchema) {
    if (tableSchema == null)
      throw new IllegalArgumentException("tableSchema cannot be null");
    if (record == null)
      throw new IllegalArgumentException("record cannot be null");

    hCatParser = new HCatParser(record, tableSchema);
  }

  private void advance() throws IOException {
    hCatParser.loadNextValue();
  }

  @Override
  public void readNull() throws IOException {
    advance();
  }

  @Override
  public boolean readBoolean() throws IOException {
    advance();
    return hCatParser.getBoolean();
  }

  @Override
  public int readInt() throws IOException {
    advance();
    return hCatParser.getInt();
  }

  @Override
  public long readLong() throws IOException {
    advance();
    return hCatParser.getLong();
  }

  @Override
  public float readFloat() throws IOException {
    advance();
    return hCatParser.getFloat();
  }

  @Override
  public double readDouble() throws IOException {
    advance();
    return hCatParser.getDouble();
  }

  @Override
  public Utf8 readString(Utf8 old) throws IOException {
    return new Utf8(readString());
  }

  @Override
  public String readString() throws IOException {
    advance();
    return hCatParser.getString();
  }

  @Override
  public void skipString() throws IOException {
    advance();
    hCatParser.getString();
  }

  @Override
  public ByteBuffer readBytes(ByteBuffer old) throws IOException {
    advance();
    Buffer byteArray = hCatParser.getByteArray();
    ByteBuffer buffer = ByteBuffer.wrap((byte[]) byteArray.array());
    return old == null ? buffer : old.put(buffer);
  }

  @Override
  public void skipBytes() throws IOException {
    advance();
    hCatParser.getByteArray();
  }

  @Override
  public void readFixed(byte[] bytes, int start, int length) throws IOException {
    advance();
    hCatParser.readFixed(bytes, start, length);
  }

  @Override
  public void skipFixed(int length) throws IOException {
    // just advance to skip over the bytes
    readFixed(new byte[length], 0, length);
  }

  @Override
  public int readEnum() throws IOException {
    advance();
    return hCatParser.getEnum();
  }

  @Override
  public long readArrayStart() throws IOException {
    advance();
    return hCatParser.getSize();
  }

  @Override
  public long arrayNext() throws IOException {
    if (hCatParser.getElementsLeft() > 0) {
      advance();
      return 1;
    }
    return 0;
  }

  @Override
  public long skipArray() throws IOException {
    return handleSkip();
  }

  @Override
  public long readMapStart() throws IOException {
    advance();
    return hCatParser.getSize();
  }

  @Override
  public long mapNext() throws IOException {
    if (hCatParser.getElementsLeft() > 0) {
      advance();
      return 1;
    }
    return 0;
  }

  @Override
  public long skipMap() throws IOException {
    return handleSkip();
  }

  @Override
  public int readIndex() throws IOException {
    // the index requested here is the index for the schema
    // within the possible union values. if a union could be
    // ["string", "null", "int"], and the actual value within
    // the union is of type string, then index should return
    // '0' indicating the position type.
    advance();
    return hCatParser.getUnionTag();
  }

  private long handleSkip() throws IOException {
    if (!hCatParser.getContext().skipping()) {
      advance();
      hCatParser.getContext().startSkipping();
      GenericDatumReader.skip(hCatParser.getContext().getSchema(), this);
    }

    if (hCatParser.getContext().done()) {
      hCatParser.getContext().stopSkipping();
      return 0;
    }

    return hCatParser.getElementsLeft();
  }
}
