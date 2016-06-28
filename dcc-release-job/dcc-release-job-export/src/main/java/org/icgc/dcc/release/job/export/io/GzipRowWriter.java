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
package org.icgc.dcc.release.job.export.io;

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.common.core.util.Joiners.PATH;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.icgc.dcc.release.core.task.TaskContext;
import org.icgc.dcc.release.job.export.function.gzip.RecordConverter;

import com.fasterxml.jackson.databind.node.ObjectNode;

@RequiredArgsConstructor
public class GzipRowWriter implements RowWriter {

  public static final String GZIP_FILES_DIR = "data";

  @NonNull
  private final String exportDir;
  @NonNull
  private final RecordConverter recordsConverter;

  @Override
  public void write(TaskContext taskContext, JavaRDD<ObjectNode> output) {
    val records = recordsConverter.convert(output);
    write(taskContext, records);
  }

  private void write(TaskContext taskContext, JavaPairRDD<String, String> output) {
    val workingDir = taskContext.getJobContext().getWorkingDir();
    checkState(taskContext.getProjectName().isPresent(), "Failed to resolve project. Incorrect task setup?");
    val projectId = taskContext.getProjectName().get();
    val outputPath = PATH.join(workingDir, exportDir, GZIP_FILES_DIR, projectId);

    output.saveAsHadoopFile(outputPath, NullWritable.class, BytesWritable.class,
        ProjectDonorMultipleGzipOutputFormat.class, GzipCodec.class);
  }

}
