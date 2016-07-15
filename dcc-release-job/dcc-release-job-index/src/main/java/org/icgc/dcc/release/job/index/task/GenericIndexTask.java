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
package org.icgc.dcc.release.job.index.task;

import static com.google.common.base.Preconditions.checkArgument;
import static org.icgc.dcc.common.core.util.Formats.formatBytes;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.checkExistence;
import static org.icgc.dcc.release.core.document.DocumentType.byFileType;
import static org.icgc.dcc.release.core.util.DocumentRDDs.combineDocumentSequenceFile;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaRDD;
import org.icgc.dcc.release.core.document.Document;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.task.GenericTask;
import org.icgc.dcc.release.core.task.TaskContext;

@Slf4j
public abstract class GenericIndexTask extends GenericTask {

  /**
   * @param size split/combine size in MBytes
   */
  protected JavaRDD<Document> readDocumnetInput(TaskContext taskContext, FileType inputFileType, int size) {
    val conf = createJobConf(taskContext);

    return readDocumentInput(taskContext, conf, inputFileType, size);
  }

  /**
   * @param size split/combine size in MBytes
   */
  protected JavaRDD<Document> readDocumentInput(TaskContext taskContext, JobConf hadoopConf, FileType inputFileType,
      int size) {
    checkArgument(taskContext.isCompressOutput(), "readDocumentInput method doesn't support reading from uncompressed "
        + "files");
    val maxFileSize = size * 1024L * 1024L;

    log.debug("Setting input split size of {}", formatBytes(maxFileSize));
    val splitSize = Long.toString(maxFileSize);
    hadoopConf.set("mapred.min.split.size", splitSize);
    hadoopConf.set("mapred.max.split.size", splitSize);

    val sparkContext = taskContext.getSparkContext();
    val path = taskContext.getPath(inputFileType);
    if (!checkExistence(taskContext.getFileSystem(), path)) {
      return taskContext.getSparkContext().emptyRDD();
    }

    return combineDocumentSequenceFile(sparkContext, path, hadoopConf, byFileType(inputFileType));
  }

}
