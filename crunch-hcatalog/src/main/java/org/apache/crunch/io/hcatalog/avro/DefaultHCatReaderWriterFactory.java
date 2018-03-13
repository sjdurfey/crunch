package org.apache.crunch.io.hcatalog.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hive.hcatalog.data.HCatRecord;

import javax.annotation.Nullable;

/**
 * <p>
 * Default implementation of the {@link HCatReaderWriterFactory}. This
 * implementation supports the ability for consumers to override how each method
 * is used by providing their own implementation. Any method call should defer
 * to that factory if it is present. This behavior can be desirable to handle
 * custom situations that the default {@link DatumReader} or {@link DatumWriter}
 * doesn't support (such as non-passive enum changes).
 * </p>
 * <p>
 * This default implementation only has implementations for {@link #getData()}
 * and {@link #getReader(Schema, Schema)}. Converting an {@link HCatRecord} into
 * an avro is not supported by default.
 * </p>
 */
public class DefaultHCatReaderWriterFactory implements HCatReaderWriterFactory {

  private final HCatToAvroType type;
  private ClassLoader specificLoader;
  private final HCatReaderWriterFactory userFactory;

  /**
   * Indicates the type of avro to use when creating datum readers and writers
   */
  public enum HCatToAvroType {
    REFLECT,
    SPECIFIC,
    GENERIC
  }

  /**
   * Constructs a new instance utilizing the desired avro {@link HCatToAvroType
   * type}.
   *
   * @param type
   *          the avro type to use when creating readers and writers
   */
  public DefaultHCatReaderWriterFactory(HCatToAvroType type) {
    this(type, null, null);
  }

  /**
   * Constructs a new instance utilizing the desired avro {@link HCatToAvroType
   * type}. An optional {@link ClassLoader} can be specified to use when
   * constructing {@link DatumReader readers} and {@link DatumWriter writers}.
   * An optional {@link HCatReaderWriterFactory factory} can be provided to
   * override default behavior.
   * 
   * @param type
   *          the avro type to use when creating readers and writers
   * @param specificLoader
   *          an optional class loader to use
   * @param factory
   *          an optional factory to override behaviors
   */
  public DefaultHCatReaderWriterFactory(HCatToAvroType type, @Nullable ClassLoader specificLoader,
      @Nullable HCatReaderWriterFactory factory) {
    if (type == null)
        throw new IllegalArgumentException("type cannot be null");
    
    this.type = type;
    this.specificLoader = specificLoader;
    this.userFactory = factory;
  }

  @Override
  public GenericData getData() {
    if (userFactory != null) {
      return userFactory.getData();
    }

    switch (type) {
    case REFLECT:
      return ReflectData.AllowNull.get();
    case SPECIFIC:
      return SpecificData.get();
    default:
      return GenericData.get();
    }
  }

  @Override
  public <D> DatumReader<D> getReader(Schema readerSchema, Schema writerSchema) {
    if (userFactory != null) {
      return userFactory.getReader(readerSchema, writerSchema);
    }

    switch (type) {
    case REFLECT:
      if (specificLoader != null) {
        return new ReflectDatumReader<>(writerSchema, readerSchema, new ReflectData(specificLoader));
      } else {
        return new ReflectDatumReader<>(writerSchema, readerSchema);
      }
    case SPECIFIC:
      if (specificLoader != null) {
        return new SpecificDatumReader<>(writerSchema, readerSchema, new SpecificData(specificLoader));
      } else {
        return new SpecificDatumReader<>(writerSchema, readerSchema);
      }
    default:
      return new GenericDatumReader<>(writerSchema, readerSchema);
    }
  }

  @Override
  public <D> DatumWriter<D> getWriter(Schema schema) {
    if (userFactory != null) {
      return userFactory.getWriter(schema);
    }

    throw new UnsupportedOperationException("Writing of HCatRecords is not supported");
  }
}
