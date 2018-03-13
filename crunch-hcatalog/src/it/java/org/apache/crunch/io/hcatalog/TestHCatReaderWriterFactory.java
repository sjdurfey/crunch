package org.apache.crunch.io.hcatalog;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.Utf8;
import org.apache.crunch.io.hcatalog.avro.HCatReaderWriterFactory;

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
