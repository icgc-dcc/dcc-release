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

import java.util.Optional;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.job.JobContext;
import org.icgc.dcc.release.core.util.Partitions;

@Slf4j
@Getter
@RequiredArgsConstructor
public class DefaultTaskContext implements TaskContext {

  @NonNull
  private final JobContext jobContext;
  @NonNull
  private final JavaSparkContext sparkContext;
  @NonNull
  private final FileSystem fileSystem;
  @NonNull
  private final Optional<String> projectName;

  @Override
  public Optional<String> getProjectName() {
    return projectName;
  }

  @Override
  public String getPath(FileType fileType) {
    val fileTypePath = new Path(jobContext.getWorkingDir(), fileType.getDirName());
    if (projectName.isPresent()) {
      val projectFileTypePath = new Path(fileTypePath, Partitions.getPartitionName(projectName.get()));

      return projectFileTypePath.toString();
    } else {
      return fileTypePath.toString();
    }
  }

  @Override
  @SneakyThrows
  public void delete(FileType fileType) {
    log.info("Deleting {} dir '{}'", fileType, getPath(fileType));
    fileSystem.delete(new Path(getPath(fileType)), true);
  }

  @Override
  @SneakyThrows
  public boolean exists(FileType fileType) {
    return fileSystem.exists(new Path(getPath(fileType)));
  }

}