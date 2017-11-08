package org.apache.hive.hcatalog.mapreduce;

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapred.TaskAttemptID;

import java.io.IOException;
import java.util.List;

/**
 * Thin extension to construct valid
 * {@link org.apache.hadoop.mapred.TaskAttemptID}, and remove the crunch named
 * output from the {@link org.apache.hadoop.mapreduce.TaskAttemptID}.
 */
public class CrunchFileOutputCommitterContainer extends FileOutputCommitterContainer {

  boolean dynamicPartitioningUsed;

  /**
   * @param context
   *          current JobContext
   * @param baseCommitter
   *          OutputCommitter to contain
   * @throws IOException
   */
  public CrunchFileOutputCommitterContainer(JobContext context, OutputCommitter baseCommitter) throws IOException {
    super(context, baseCommitter);
    dynamicPartitioningUsed = HCatOutputFormat.getJobInfo(context.getConfiguration()).isDynamicPartitioningUsed();
  }

  @Override
  public void setupTask(TaskAttemptContext context) throws IOException {
    if (!dynamicPartitioningUsed) {
      getBaseOutputCommitter().setupTask(getOldTaskAttemptContext(context));
    }
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext context) throws IOException {
    if (!dynamicPartitioningUsed) {
      return getBaseOutputCommitter().needsTaskCommit(getOldTaskAttemptContext(context));
    } else {
      // called explicitly through FileRecordWriterContainer.close() if dynamic
      // - return false by default
      return true;
    }
  }

  @Override
  public void commitTask(TaskAttemptContext context) throws IOException {
    if (!dynamicPartitioningUsed) {
      // See HCATALOG-499
      FileOutputFormatContainer.setWorkOutputPath(context);
      getBaseOutputCommitter().commitTask(getOldTaskAttemptContext(context));
    } else {
      try {
        TaskCommitContextRegistry.getInstance().commitTask(context);
      } finally {
        TaskCommitContextRegistry.getInstance().discardCleanupFor(context);
      }
    }
  }

  @Override
  public void abortTask(TaskAttemptContext context) throws IOException {
    if (!dynamicPartitioningUsed) {
      getBaseOutputCommitter().abortTask(getOldTaskAttemptContext(context));
    } else {
      try {
        TaskCommitContextRegistry.getInstance().abortTask(context);
      } finally {
        TaskCommitContextRegistry.getInstance().discardCleanupFor(context);
      }
    }
  }

  private org.apache.hadoop.mapred.TaskAttemptContext getOldTaskAttemptContext(TaskAttemptContext context) {
    return new TaskAttemptContextImpl(new JobConf(context.getConfiguration()), getTaskAttemptID(context));
  }

  // The task attempt id might have the crunch named output in it. This gives
  // the id 7 parts. When constructing the TaskAttemptID, it ensures there are
  // only 6 parts. Remove the named output from the task attempt id so a valid
  // id can be created
  TaskAttemptID getTaskAttemptID(TaskAttemptContext context) {
    String taskAttemptId = context.getTaskAttemptID().toString();
    List<String> taskAttemptIDParts = Lists.newArrayList(taskAttemptId.split("_"));
    if (taskAttemptIDParts.size() == 6)
      return org.apache.hadoop.mapred.TaskAttemptID.forName(taskAttemptId);

    // index 2 is the 3rd element in the task attempt id, which will be the
    // named output
    taskAttemptIDParts.remove(2);
    String reducedTaskAttemptId = StringUtils.join(taskAttemptIDParts, "_");
    return org.apache.hadoop.mapred.TaskAttemptID.forName(reducedTaskAttemptId);
  }
}
