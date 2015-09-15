package org.icgc.dcc.release.core.job;

import java.util.Collection;
import java.util.List;

import lombok.Value;

import org.apache.hadoop.fs.Path;
import org.icgc.dcc.release.core.task.Task;
import org.icgc.dcc.release.core.task.TaskExecutor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Table;

@Value
public class DefaultJobContext implements JobContext {

  JobType type;
  String releaseName;
  List<String> projectNames;

  String releaseDir;
  String workingDir;

  Table<String, String, List<Path>> files;

  TaskExecutor executor;

  @Override
  public void execute(Task... tasks) {
    execute(ImmutableList.copyOf(tasks));
  }

  @Override
  public void execute(Collection<? extends Task> tasks) {
    executor.execute(this, tasks);
  }

}
