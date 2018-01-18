package org.apache.crunch.io.hcatalog.avro;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.Buffer;
import java.sql.Date;
import java.sql.Timestamp;

/**
 * A parser for an {@link HCatRecord}. It traverses one value at a time and
 * creates a graph during the traversal. If a new complex type is encountered
 * (e.g. struct, list, map, union), a new node is created in the graph, and
 * traversal will continue for each element within that node. Retrieval of
 * values from this parser as such will have a very strict ordering. Random
 * access for elements within the {@link HCatRecord} is not provided.
 *
 * This class is not thread safe.
 */
public class HCatParser {

  private static final Logger LOGGER = LoggerFactory.getLogger(HCatParser.class);

  private HCatParseContext context;
  private Object currValue;

  public HCatParser(HCatRecord record, Schema tableSchema) {
    if (tableSchema == null)
      throw new IllegalArgumentException("tableSchema cannot be null");
    if (record == null)
      throw new IllegalArgumentException("record cannot be null");

    context = HCatParseContext.createRootContext(record, tableSchema);
  }

  public HCatParseContext getContext() {
    return context;
  }

  /**
   * Advances to the next value with the record being parsed.
   * 
   * @throws IllegalStateException
   *           if more values were requested, but the record has been completely
   *           traversed
   */
  public void loadNextValue() {
    if (context == null) {
      // if context is null it means that the record has been completely
      // traversed. there's nowhere to go, so throw exception
      throw new IllegalStateException("Record traversal is done, but more values were requested");
    }

    // if the current context is done, then walk
    // backwards through the graph to the last
    // context that wasn't finished processing
    if (context.done())
      context = findUnfinishedParent();

    if (doneProcessing())
      throw new IllegalStateException("Record traversal is done, but more values were requested");

    currValue = context.nextValue();
    maybeCreateChildContext();
  }

  /**
   * Identifies the index within the list of possible types for this union. For
   * example, if a union has the following possible types:
   *
   * <pre>
   *     ['string', 'null', 'int']
   * </pre>
   *
   * And the value in the union is of type 'string', then this method will
   * return 0. If it is an integer, then it will return 2
   *
   * @return
   * @throws AvroTypeException
   *           if a union tag was requested but the current context isn't a
   *           union
   */
  public int getUnionTag() {
    if (context instanceof HCatParseUnionContext) {
      return ((HCatParseUnionContext) context).getTag();
    }

    String msg = "The current type is " + context.getValueTypeInfo().getTypeName() + " which isn't compatible with the"
        + " list of possible types for this union: " + StringUtils.join(context.getSchema().getTypes().toArray());

    throw new AvroTypeException(msg);
  }

  /**
   * The size of the current context (e.g. num elements in the map or list).
   * This needed for retrieving the size for get array and get map starts in the
   * decoder.
   * 
   * @return
   */
  public long getSize() {
    return context.getSize();
  }

  /**
   * Indicates to the decoder how many elements are left to process
   * 
   * @return
   */
  public long getElementsLeft() {
    return context.getRemainingSize();
  }

  /**
   * Returns the integer value or null if the field doesn't exist
   *
   * @return the integer value or null
   */
  public Integer getInt() {
    return _getField(Integer.class);
  }

  /**
   * Reads a fixed number of bytes into the passed in {@code bytes} array
   * 
   * @param bytes
   *          the array to read into
   * @param start
   *          where to start in the {@code bytes} array
   * @param length
   *          the length of {@code bytes}
   */
  public void readFixed(byte[] bytes, int start, int length) {
    GenericData.Fixed fixed = _getField(GenericData.Fixed.class);
    System.arraycopy(fixed.bytes(), 0, bytes, start, length);
  }

  /**
   * Returns the enum ordinal for the current value
   * 
   * @return
   */
  public int getEnum() {
    // get the string value
    String enumStr = _getField(String.class);
    // get the value schema to retrieve the enum ordinal
    Schema valueSchema = context.getValueSchema();
    return valueSchema.getEnumOrdinal(enumStr);
  }

  /**
   * Returns the string value or null if the field doesn't exist
   *
   * @return the string value or null
   */
  public String getString() {
    return _getField(String.class);
  }

  /**
   * Returns the Boolean value or null if the field doesn't exist
   *
   * @return the boolean value or null
   */
  public Boolean getBoolean() {
    return _getField(Boolean.class);
  }

  /**
   * Returns the byte array value or null if the field doesn't exist
   *
   * @return the byte array value or null
   */
  public Buffer getByteArray() {
    return _getField(Buffer.class);
  }

  /**
   * Returns the long value or null if the field doesn't exist
   *
   * @return the long value or null
   */
  public Long getLong() {
    return _getField(Long.class);
  }

  /**
   * Returns the double value or null if the field doesn't exist
   *
   * @return the double value or null
   */
  public Double getDouble() {
    return _getField(Double.class);
  }

  /**
   * Returns the float value or null if the field doesn't exist
   *
   * @return the float value or null
   */
  public Float getFloat() {
    return _getField(Float.class);
  }

  private boolean doneProcessing() {
    // processing is finished if we are at the root
    // context (which won't have a parent), and that context
    // is done. if context is null, we've already reached
    // the end of traversal
    return context == null || (context.getParent() == null && context.done());
  }

  // finds the next position in a context.
  private HCatParseContext findUnfinishedParent() {
    HCatParseContext temp = context;
    // null parent indicates we are at the root context and have completely
    // un-recursed out of sub-trees. return that context so null isn't returned
    // or un-recurse until a context is found that isn't complete. continue
    // processing from there
    while (temp.getParent() != null && temp.done()) {
      temp = temp.getParent();
    }

    return temp;
  }

  private <T> T _getField(Class<T> clazz) throws AvroTypeException {
    // situation could arise where a map or a list (which currValue could be)
    // hasn't been started yet (where started is defined as values haven't been
    // read from it yet). if it hasn't started, then call next value to get the
    // value, and call back to this method. now the context should be started,
    // so move on to conversion
    if (!context.started()) {
      loadNextValue();
      _getField(clazz);
    }

    Object primitive = convertPrimitive();

    // Decoder API dictates an AvroTypeException should be thrown
    // if the requested type doesn't match the returned type
    if (!clazz.isAssignableFrom(primitive.getClass()))
      throw new AvroTypeException("Expected type [" + clazz + "] but found type [" + currValue.getClass() + "]");

    return (T) primitive;
  }

  // maybe create a new context. if the current value is one of: map, list,
  // struct, or union, then a child context needs to be created, pointing to its
  // parent. this treats the contents of the original HCatRecord like its a
  // graph and the linking allows for traversal (down and back to
  // findUnfinishedParent)
  private boolean maybeCreateChildContext() {
    // a union has special treatment associated with it. an avro union
    // isnt represented as a union in hive, but just as a value.
    // create a union context here and let that class worry about
    // how to handle the avro union
    if (context.getValueSchema().getType() == Schema.Type.UNION) {
      context = context.createChildUnionContext(currValue, context.getValueTypeInfo(),
          context.getValueObjectInspector(), context.getValueSchema());
      return true;
    }

    switch (context.getValueTypeInfo().getCategory()) {
    case MAP:
      context = context.createChildMapContext(currValue, context.getValueTypeInfo(), context.getValueObjectInspector(),
          context.getValueSchema());
      return true;
    case STRUCT:
      context = context.createChildStructContext(currValue, context.getValueTypeInfo(),
          context.getValueObjectInspector(), context.getValueSchema());
      return true;
    case LIST:
      context = context.createChildListContext(currValue, context.getValueTypeInfo(), context.getValueObjectInspector(),
          context.getValueSchema());
      return true;
    case UNION:
      context = context.createChildUnionContext(currValue, context.getValueTypeInfo(),
          context.getValueObjectInspector(), context.getValueSchema());
      break;
    }

    return false;
  }

  /**
   * Converts the current primitive into the appropriate avro type. Code
   * borrowed (with minor change) from the hcatalog project.
   * 
   * @see <a href=
   *      "https://github.com/apache/hive/blob/rel/release-2.1.0/serde/src/java/org/apache/hadoop/hive/serde2/avro/AvroSerializer.java#L191">Source
   *      of code</a>
   * 
   * @return
   * @throws AvroTypeException
   *           if the requested type didn't match the current value
   */
  private Object convertPrimitive() throws AvroTypeException {
    TypeInfo typeInfo = context.getValueTypeInfo();
    PrimitiveObjectInspector fieldOI = (PrimitiveObjectInspector) context.getValueObjectInspector();
    Schema schema = context.getValueSchema();

    switch (fieldOI.getPrimitiveCategory()) {
    case BINARY:
      if (schema.getType() == Schema.Type.BYTES) {
        return AvroSerdeUtils.getBufferFromBytes((byte[]) fieldOI.getPrimitiveJavaObject(currValue));
      } else if (schema.getType() == Schema.Type.FIXED) {
        SpecificData.Fixed fixed = new SpecificData.Fixed(schema, (byte[]) fieldOI.getPrimitiveJavaObject(currValue));
        return fixed;
      } else {
        throw new AvroTypeException("Unexpected Avro schema for Binary TypeInfo: " + schema.getType());
      }
    case DECIMAL:
      HiveDecimal dec = (HiveDecimal) fieldOI.getPrimitiveJavaObject(currValue);
      return AvroSerdeUtils.getBufferFromDecimal(dec, ((DecimalTypeInfo) typeInfo).scale());
    case CHAR:
      HiveChar ch = (HiveChar) fieldOI.getPrimitiveJavaObject(currValue);
      return ch.getStrippedValue();
    case VARCHAR:
      HiveVarchar vc = (HiveVarchar) fieldOI.getPrimitiveJavaObject(currValue);
      return vc.getValue();
    case DATE:
      Date date = ((DateObjectInspector) fieldOI).getPrimitiveJavaObject(currValue);
      return DateWritable.dateToDays(date);
    case TIMESTAMP:
      Timestamp timestamp = ((TimestampObjectInspector) fieldOI).getPrimitiveJavaObject(currValue);
      return timestamp.getTime();
    case UNKNOWN:
      throw new AvroTypeException("Received UNKNOWN primitive category.");
    case VOID:
      return null;
    default: // All other primitive types are simple
      return fieldOI.getPrimitiveJavaObject(currValue);
    }
  }
}
