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

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.icgc.dcc.release.core.util.Stopwatches;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;

@Slf4j
public abstract class RemoteActionTask implements Task {

  @Override
  public void execute(TaskContext taskContext) {
    val fsUri = taskContext.getFileSystem().getUri();
    val workingDir = taskContext.getJobContext().getWorkingDir();

    // Note: Can't use val in lambda (e.g. watch, fileSystem), don't try!
    executeRemote(taskContext.getSparkContext(), ignore -> {
      Stopwatch watch = Stopwatches.createStarted();
      log.info("Executing remote action...");

      FileSystem fileSystem = FileSystem.get(fsUri, new Configuration());
      executeRemoteAction(fileSystem, new Path(workingDir));
      log.info("Finished executing action in {}", watch);

      return ignore;
    });
  }

  private void executeRemote(JavaSparkContext sparkContext, Function<Integer, Integer> function) {
    val one = 1;
    val task = ImmutableList.of(one);
    val numSlices = one;

    sparkContext.parallelize(task, numSlices).map(function).count();
  }

  /**
   * Template method. Subclasses are required to implement an action to be taken cluster side.
   */
  protected abstract void executeRemoteAction(FileSystem fileSystem, Path workingDir);

}
