/*
 * Copyright (c) 2015 The Ontario Institute for Cancer Research. All rights reserved.                             
 *                                                                                                               
 * This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
 * You should have received a copy of the GNU General Public License along with                                  
 * this program. If not, see <http://www.gnu.org/licenses/>.                                                     
 *                                                                                                               
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY                           
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES                          
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT                           
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,                                
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED                          
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;                               
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER                              
 * IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN                         
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.icgc.dcc.release.core.task;

import static com.google.common.base.Stopwatch.createStarted;
import static com.google.common.base.Throwables.propagate;

import java.util.Collection;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaSparkContext;
import org.icgc.dcc.release.core.job.JobContext;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

@Slf4j
@RequiredArgsConstructor
public class TaskExecutor {

  /**
   * Dependencies.
   */
  @NonNull
  protected final ExecutorService executor;
  @NonNull
  protected final JavaSparkContext sparkContext;
  @NonNull
  protected final FileSystem fileSystem;

  public void execute(@NonNull JobContext jobContext, Collection<? extends Task> tasks) {
    execute(jobContext, tasks, true);
  }

  public void executeSequentially(@NonNull JobContext jobContext, Collection<? extends Task> tasks) {
    execute(jobContext, tasks, false);
  }

  public void shutdown() {
    log.info("Cancelling all tasks...");
    sparkContext.cancelAllJobs();
    log.info("Cancelled all tasks");
  }

  private void execute(JobContext jobContext, Collection<? extends Task> tasks, boolean parallel) {
    val watch = createStarted();
    try {
      log.info("Starting {} task(s)...", tasks.size());
      executeTasks(jobContext, tasks, parallel);
      log.info("Finished {} task(s) in {}", tasks.size(), watch);
    } catch (Throwable t) {
      log.error("Aborting task(s) executions due to exception...", t);
      propagate(t);
    }
  }

  @SneakyThrows
  private int executeTasks(JobContext jobContext, Collection<? extends Task> tasks, boolean parallel) {
    val service = createCompletionService();
    val watch = createStarted();
    int taskCount = 0;
    val submitTasks = getSubmitTasks(jobContext, tasks);

    for (val entry : submitTasks) {
      val task = entry.getKey();
      val projectName = entry.getValue();
      submitTask(service, jobContext, task, projectName);
      taskCount++;

      if (!parallel) {
        await(service);
      }
    }

    if (parallel) {
      await(service, taskCount);
    }
    log.info("Finished executing {} tasks in {}!", taskCount, watch);

    return taskCount;
  }

  private static Collection<Entry<? extends Task, Optional<String>>> getSubmitTasks(JobContext jobContext,
      Collection<? extends Task> tasks) {
    val submitTasks = ImmutableList.<Entry<? extends Task, Optional<String>>> builder();
    for (val task : tasks) {
      if (task.getType() == TaskType.FILE_TYPE_PROJECT) {
        for (val projectName : jobContext.getProjectNames()) {
          submitTasks.add(Maps.immutableEntry(new ProjectTask(task, projectName), Optional.of(projectName)));
        }
      } else {
        submitTasks.add(Maps.immutableEntry(task, Optional.empty()));
      }
    }

    return submitTasks.build();
  }

  private void submitTask(CompletionService<String> service, JobContext jobContext, Task task,
      Optional<String> projectName) {
    log.info("Submitting '{}' task...", task.getName());
    val taskContext = createTaskContext(jobContext, projectName);

    // Submit async
    service.submit(() -> {
      Stopwatch watch = createStarted();
      prepareSubmission(task);

      try {
        task.execute(taskContext);
      } catch (Exception e) {
        log.error("Failed to execute task '{}'", task.getName());
        throw e;
      }

      return task.getName() + " - " + watch;
    });
  }

  private void prepareSubmission(Task task) {
    val interrupt = true;
    val description = "Task of type " + task.getType();

    sparkContext.setJobGroup(task.getName(), description, interrupt);
    setPriority(task.getPriority());
  }

  private void setPriority(TaskPriority priority) {
    // This setting is thread local. It will be visible only to the current task.
    // See http://spark.apache.org/docs/latest/job-scheduling.html#fair-scheduler-pools
    sparkContext.setLocalProperty("spark.scheduler.pool", priority.getPool());
  }

  private TaskContext createTaskContext(JobContext jobContext, Optional<String> projectName) {
    return new DefaultTaskContext(jobContext, sparkContext, fileSystem, projectName, jobContext.isCompressOutput());
  }

  private ExecutorCompletionService<String> createCompletionService() {
    return new ExecutorCompletionService<String>(executor);
  }

  private void await(CompletionService<String> service, int count) throws InterruptedException, ExecutionException {
    // This will iterate in completion order, failing fast if there is a protect task failure
    for (int i = 0; i < count; i++) {
      val taskName = service.take().get();
      log.info("Finished processing task '{}'", taskName);
    }
  }

  private static void await(CompletionService<String> service) throws InterruptedException, ExecutionException {
    val taskName = service.take().get();
    log.info("Finished processing task '{}'", taskName);
  }

  private static class ProjectTask extends ForwardingTask {

    private final String projectName;

    private ProjectTask(@NonNull Task delegate, @NonNull String projectName) {
      super(delegate);
      this.projectName = projectName;
    }

    @Override
    public String getName() {
      return Task.getName(super.getName(), projectName);
    }

  }

}
