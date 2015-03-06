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
package org.icgc.dcc.etl2.core.task;

import static com.google.common.base.Throwables.propagate;
import static org.icgc.dcc.etl2.core.util.Stopwatches.createStarted;

import java.util.Collection;
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
import org.icgc.dcc.etl2.core.job.JobContext;

import com.google.common.collect.ImmutableList;

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

  public void execute(@NonNull JobContext jobContext, Task... tasks) {
    execute(jobContext, ImmutableList.copyOf(tasks));
  }

  public void execute(@NonNull JobContext jobContext, Collection<Task> tasks) {
    val watch = createStarted();
    try {
      log.info("Starting '{}' tasks...", tasks.size());

      executeTasks(jobContext, tasks);

      log.info("Finished '{}' tasks in {}", tasks.size(), watch);
    } catch (Throwable t) {
      log.error("Aborting task executions due to exception...", t);
      propagate(t);
    }
  }

  @SneakyThrows
  private int executeTasks(JobContext jobContext, Collection<Task> tasks) {
    val service = createCompletionService();

    val watch = createStarted();
    int taskCount = 0;
    for (val task : tasks) {
      if (task.getType() == TaskType.FILE_TYPE_PROJECT) {
        submitProjectTasks(service, jobContext, task);
      } else {
        submitTask(service, jobContext, task);
      }

      taskCount++;
    }

    await(service, taskCount);
    log.info("Finished executing {} tasks in {}!", taskCount, watch);

    return taskCount;
  }

  private void submitProjectTasks(CompletionService<String> service, JobContext jobContext, Task task) {
    for (val projectName : jobContext.getProjectNames()) {
      submitProjectTask(service, jobContext, task, projectName);
    }
  }

  private void submitProjectTask(CompletionService<String> service, JobContext jobContext, Task projectTask,
      String projectName) {
    val projectTaskName = projectTask.getName() + ":" + projectName;

    log.info("Submitting '{}' task...", projectTaskName);
    val taskContext = createTaskContext(jobContext, Optional.of(projectName));

    // Submit project task async
    service.submit(() -> {
      projectTask.execute(taskContext);

      return projectTaskName;
    });
  }

  private void submitTask(CompletionService<String> service, JobContext jobContext, Task task) {
    log.info("Submitting '{}' task...", task.getName());
    val taskContext = createTaskContext(jobContext, Optional.empty());

    // Submit async
    service.submit(() -> {
      task.execute(taskContext);

      return task.getName();
    });
  }

  private TaskContext createTaskContext(JobContext jobContext, Optional<String> projectName) {
    return new DefaultTaskContext(jobContext, sparkContext, fileSystem, projectName);
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

}
