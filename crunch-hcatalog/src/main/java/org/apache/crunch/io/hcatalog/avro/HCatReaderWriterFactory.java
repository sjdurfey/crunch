package org.apache.crunch.io.hcatalog.avro;

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
