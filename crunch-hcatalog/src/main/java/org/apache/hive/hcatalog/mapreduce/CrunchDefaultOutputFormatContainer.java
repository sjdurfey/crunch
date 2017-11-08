package org.apache.hive.hcatalog.mapreduce;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author Stephen Durfey
 */
public class CrunchDefaultOutputFormatContainer extends DefaultOutputFormatContainer {

  public CrunchDefaultOutputFormatContainer(org.apache.hadoop.mapred.OutputFormat<WritableComparable<?>, Writable> of) {
    super(of);
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
    return new CrunchDefaultOutputCommitterContainer(context,
        new JobConf(context.getConfiguration()).getOutputCommitter());
  }
}
