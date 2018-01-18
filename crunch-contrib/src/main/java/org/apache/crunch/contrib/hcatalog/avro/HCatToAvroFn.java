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
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.MapFn;
import org.apache.commons.lang3.StringUtils;
import org.apache.crunch.io.hcatalog.HCatSourceTarget;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.crunch.contrib.hcatalog.HCatAvroSourceTarget.AVRO_TABLE_SCHEMA_PROP;
import static org.apache.crunch.contrib.hcatalog.HCatAvroSourceTarget.DATUM_READER_WRITER_FACTORY_PROP;
import static org.apache.crunch.contrib.hcatalog.avro.DefaultHCatReaderWriterFactory.HCatToAvroType;

/**
 * <p>
 * A {@link MapFn} for converting {@link HCatRecord}'s into avro classes of type
 * {@link SpecificRecord}. The reader class is required to retrieve the reader
 * schema. The writer schema is pulled from configuration that was populated by
 * the {@link HCatSourceTarget}, which was pulled from the hive table
 * definition.
 * </p>
 * <p>
 * This function utilizes an instance of the {@link HCatReaderWriterFactory}. A
 * consumer can override the default behavior by specifying their own instance
 * of the factory by setting the canonical class name in the job configuration
 * by setting with the {@link HCatAvroSourceTarget#DATUM_READER_WRITER_FACTORY_PROP}
 * property.
 * </p>
 * 
 * @param <T> an avro subclass of type {@link SpecificRecord}
 */
public class HCatToAvroFn<T extends SpecificRecord> extends MapFn<HCatRecord, T> {

  private static final Logger LOGGER = LoggerFactory.getLogger(HCatToAvroFn.class);
  private final Class<T> readerClazz;
  private transient Schema readerSchema;
  private transient Schema tableSchema;
  private transient DatumReader<T> datumReader;

  /**
   * Instantiates a new instance, and configures the conversion to use the
   * specified {@code readerClazz}.
   * 
   * @param readerClazz
   *          the avro class for conversion
   * @throws IllegalArgumentException if {@code readerClazz} is null
   */
  public HCatToAvroFn(Class<T> readerClazz) {
    if (readerClazz == null)
      throw new IllegalArgumentException("readerClazz cannot be null");

    this.readerClazz = readerClazz;
  }

  @Override
  public void initialize() {
    Configuration conf = getConfiguration();
    String tableSchema = conf.get(AVRO_TABLE_SCHEMA_PROP);
    if (StringUtils.isEmpty(tableSchema))
      throw new CrunchRuntimeException(
          "Configuration property [" + AVRO_TABLE_SCHEMA_PROP + "] is needed for conversion. Is the table an avro table?");

    HCatReaderWriterFactory userFactory = null;
    try {
      String prop = conf.get(DATUM_READER_WRITER_FACTORY_PROP);
      if (StringUtils.isNotEmpty(prop)) {
        LOGGER.debug("Found config [{}] and will use this factory for reader/writer creation", prop);
        Class<?> datumClass = Class.forName(prop);
        userFactory = (HCatReaderWriterFactory) ReflectionUtils.newInstance(datumClass, conf);
      } else {
        LOGGER.debug("Customer reader/writer factory not found in config [{}]", DATUM_READER_WRITER_FACTORY_PROP);
      }
    } catch (ClassNotFoundException e) {
      throw new CrunchRuntimeException(e);
    }

    HCatReaderWriterFactory factory = new DefaultHCatReaderWriterFactory(HCatToAvroType.SPECIFIC,
            readerClazz.getClassLoader(), userFactory);

    this.tableSchema = new Schema.Parser().parse(tableSchema);
    readerSchema = SpecificData.get().getSchema(readerClazz);
    datumReader = factory.getReader(readerSchema, this.tableSchema);
  }

  protected Class<T> getClass(String targetClass) {
    try {
      return (Class<T>) Class.forName(targetClass);
    } catch (ReflectiveOperationException e) {
      throw new CrunchRuntimeException(e);
    }
  }

  @Override
  public T map(HCatRecord input) {
    try {
      HCatDecoder decoder = new HCatDecoder(input, tableSchema);
      return datumReader.read(null, decoder);
    } catch (IOException e) {
      throw new CrunchRuntimeException(e);
    }
  }
}
