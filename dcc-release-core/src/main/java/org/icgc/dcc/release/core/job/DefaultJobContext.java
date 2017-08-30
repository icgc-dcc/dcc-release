/*
 * Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.release.core.job;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;

import lombok.Value;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.icgc.dcc.release.core.task.Task;
import org.icgc.dcc.release.core.task.TaskExecutor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Table;

import lombok.AllArgsConstructor;

@Value
@AllArgsConstructor
public class DefaultJobContext implements JobContext {

  JobType type;
  String releaseName;
  List<String> projectNames;

  List<String> releaseDirs; // input/submission
  String workingDir; // output

  Table<String, String, List<Path>> files;

  TaskExecutor executor;

  boolean compressOutput;

  @Override
  public void execute(Task... tasks) {
    execute(ImmutableList.copyOf(tasks));
  }

  @Override
  public void execute(Collection<? extends Task> tasks) {
    executor.execute(this, tasks);
  }

  @Override
  public void executeSequentially(Task... tasks) {
    executeSequentially(ImmutableList.copyOf(tasks));
  }

  @Override
  public void executeSequentially(Collection<? extends Task> tasks) {
    executor.executeSequentially(this, tasks);
  }

  @Override
  public JavaSparkContext getJavaSparkContext() {
    return executor.getSparkContext();
  }

  @Override
  public FileSystem getFileSystem() {
    return executor.getFileSystem();
  }

  @Override
  public void execute(ExecutorService executorService, Task... tasks) {
    executor.execute(this, ImmutableList.copyOf(tasks), executorService);
  }

  @Override
  public void execute(ExecutorService executorService, Collection<? extends Task> tasks) {
    executor.execute(this, tasks, executorService);
  }

  @Override
  public void executeSequentially(ExecutorService executorService, Task... tasks) {
    executor.executeSequentially(this, ImmutableList.copyOf(tasks), executorService);
  }

  @Override
  public void executeSequentially(ExecutorService executorService, Collection<? extends Task> tasks) {
    executor.executeSequentially(this, tasks, executorService);
  }

}
