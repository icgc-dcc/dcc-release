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
package org.icgc.dcc.release.job.legacy.export.task;

import static org.icgc.dcc.release.job.legacy.export.core.LegacyExportJob.EXPORT_DIR;
import static org.icgc.dcc.release.job.legacy.export.core.LegacyExportJob.getOutputFileType;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.task.GenericTask;
import org.icgc.dcc.release.core.task.TaskContext;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hadoop.compression.lzo.LzopCodec;

@RequiredArgsConstructor
public class LegacyExportTask extends GenericTask {

  private final FileType fileType;

  @Override
  public void execute(TaskContext taskContext) {
    val output = readInput(taskContext, fileType);
    val outputPath = getOutputPath(taskContext, getOutputFileType(fileType));

    writeLzoOutput(output, outputPath);
  }

  private void writeLzoOutput(JavaRDD<ObjectNode> input, String outputPath) {
    input.saveAsTextFile(outputPath, LzopCodec.class);
  }

  private static String getOutputPath(TaskContext taskContext, FileType fileType) {
    val workingDir = taskContext.getJobContext().getWorkingDir();
    val fileTypePath = new Path(new Path(new Path(workingDir), EXPORT_DIR), fileType.getDirName());

    taskContext.getPath(fileType);
    val projectName = taskContext.getProjectName();
    if (projectName.isPresent()) {
      val projectFileTypePath = new Path(fileTypePath, projectName.get());

      return projectFileTypePath.toString();
    } else {
      return fileTypePath.toString();
    }
  }

}
