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

import static org.icgc.dcc.release.core.task.TaskType.FILE_TYPE;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;

import lombok.val;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaSparkContext;
import org.icgc.dcc.release.core.job.DefaultJobContext;
import org.icgc.dcc.release.core.job.JobType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;

@RunWith(MockitoJUnitRunner.class)
public class TaskExecutorTest {

  @Mock
  JavaSparkContext sparkContext;
  @Mock
  FileSystem fileSystem;

  ExecutorService executorService = MoreExecutors.sameThreadExecutor();

  @Test
  public void testExecute() {
    val jobContext = new DefaultJobContext(JobType.STAGE,
        "",
        ImmutableList.<String> of(),
        Arrays.asList(""),
        "",
        null,
        new TaskExecutor(executorService, sparkContext, fileSystem),
        false);

    jobContext.execute(
        task(() -> System.out.println("task 1")),
        task(() -> System.out.println("task 2")));
  }

  private static Task task(Runnable runnable) {
    return new Task() {

      @Override
      public TaskType getType() {
        return FILE_TYPE;
      }

      @Override
      public void execute(TaskContext taskContext) {
        runnable.run();
      }

    };
  }

}
