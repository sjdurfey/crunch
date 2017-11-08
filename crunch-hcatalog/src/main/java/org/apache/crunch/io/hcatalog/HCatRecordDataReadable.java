package org.apache.crunch.io.hcatalog;

import com.google.common.base.Objects;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.ReadableData;
import org.apache.crunch.SourceTarget;
import org.apache.crunch.io.FormatBundle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class HCatRecordDataReadable implements ReadableData<HCatRecord> {

  private final FormatBundle<HCatInputFormat> bundle;
  private final String database;
  private final String table;
  private final String filter;

  public HCatRecordDataReadable(FormatBundle<HCatInputFormat> bundle, String database, String table, String filter) {
    this.bundle = bundle;
    this.database = database;
    this.table = table;
    this.filter = filter;
  }

  @Override
  public Set<SourceTarget<?>> getSourceTargets() {
    return null;
  }

  @Override
  public void configure(Configuration conf) {
    // need to configure the input format, so the JobInputInfo is populated with
    // the partitions to be processed. the partitions are needed to derive the
    // input splits and to get a size estimate for the HCatSource.
    Configuration jobConf = new Configuration(conf);
    try {
      HCatInputFormat.setInput(jobConf, database, table, filter);
    } catch (IOException e) {
      throw new CrunchRuntimeException(e);
    }
    // sets the configs added by the HCatInputFormat to the bundle. the bundle
    // will then configure in the future with this configs
    for (Map.Entry<String, String> e : jobConf) {
      String key = e.getKey();
      String value = e.getValue();
      if (!Objects.equal(value, conf.get(key))) {
        bundle.set(key, value);
      }
    }
  }

  @Override
  public Iterable<HCatRecord> read(TaskInputOutputContext<?, ?, ?, ?> context) throws IOException {
    return new HCatRecordDataIterable(bundle, context.getConfiguration());
  }
}
