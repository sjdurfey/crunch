package org.apache.crunch.io.hcatalog.avro;

import org.apache.avro.Schema;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * A context for processing a list.
 */
public class HCatParseListContext extends HCatParseContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(HCatParseListContext.class);
  // maintains the position within the list for processing
  private int index;
  // the list data
  private final List<?> list;

  public HCatParseListContext(HCatParseContext parent, Object obj, TypeInfo typeInfo, ObjectInspector inspector,
      Schema schema) {
    super(parent, schema);

    ListObjectInspector listObjectInspector = (ListObjectInspector) inspector;
    currentValueObjectInspector = listObjectInspector.getListElementObjectInspector();
    list = listObjectInspector.getList(obj);
    currentTypeInfo = ((ListTypeInfo) typeInfo).getListElementTypeInfo();
    index = 0;
    size = remainingElements = list.size();
    currentFieldSchema = schema.getElementType();
  }

  @Override
  public boolean done() {
    return index >= size;
  }

  @Override
  public Object nextValue() {
    if (done())
      throw new IllegalStateException("Another value was requested, but context is finished");

    LOGGER.debug("Retrieving element [{}] in a list of size [{}]", index, size);

    Object obj = list.get(index);
    index++;
    remainingElements--;
    return obj;
  }
}
