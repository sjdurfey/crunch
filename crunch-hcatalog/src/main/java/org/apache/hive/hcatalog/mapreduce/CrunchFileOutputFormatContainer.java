package org.apache.hive.hcatalog.mapreduce;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Thin extention to return an {@link OutputCommitter} that carries out crunch
 * specific logic
 */
public class CrunchFileOutputFormatContainer extends FileOutputFormatContainer {

  /**
   * @param of
   *          base OutputFormat to contain
   */
  public CrunchFileOutputFormatContainer(OutputFormat<? super WritableComparable<?>, ? super Writable> of) {
    super(of);
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
    // this needs to be manually set, under normal circumstances MR Task does
    // this
    setWorkOutputPath(context);
    return new CrunchFileOutputCommitterContainer(context,
        HCatBaseOutputFormat.getJobInfo(context.getConfiguration()).isDynamicPartitioningUsed() ? null
            : new JobConf(context.getConfiguration()).getOutputCommitter());
  }
}
