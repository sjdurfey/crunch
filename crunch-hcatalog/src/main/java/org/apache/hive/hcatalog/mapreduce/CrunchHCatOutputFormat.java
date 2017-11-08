package org.apache.hive.hcatalog.mapreduce;

import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.HCatRecord;

import java.io.IOException;

/**
 * Thin extension to supply {@link OutputFormat}'s that carry out crunch
 * specific logic
 */
public class CrunchHCatOutputFormat extends HCatOutputFormat {

  @Override
  protected OutputFormat<WritableComparable<?>, HCatRecord> getOutputFormat(JobContext context) throws IOException {
    OutputJobInfo jobInfo = getJobInfo(context.getConfiguration());
    HiveStorageHandler storageHandler = HCatUtil.getStorageHandler(context.getConfiguration(),
        jobInfo.getTableInfo().getStorerInfo());
    // Always configure storage handler with jobproperties/jobconf before
    // calling any methods on it
    configureOutputStorageHandler(context);
    if (storageHandler instanceof FosterStorageHandler) {
      return new CrunchFileOutputFormatContainer(
          ReflectionUtils.newInstance(storageHandler.getOutputFormatClass(), context.getConfiguration()));
    } else {
      return new CrunchDefaultOutputFormatContainer(
          ReflectionUtils.newInstance(storageHandler.getOutputFormatClass(), context.getConfiguration()));
    }
  }
}
