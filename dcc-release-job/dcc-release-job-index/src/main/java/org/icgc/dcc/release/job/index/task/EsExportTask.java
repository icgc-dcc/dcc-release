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

import java.util.Map;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.icgc.dcc.common.hadoop.fs.HadoopUtils;
import org.icgc.dcc.release.core.document.Document;
import org.icgc.dcc.release.core.document.DocumentType;
import org.icgc.dcc.release.core.task.TaskContext;
import org.icgc.dcc.release.core.task.TaskType;
import org.icgc.dcc.release.core.util.Configurations;
import org.icgc.dcc.release.job.index.function.CreateDocument;
import org.icgc.dcc.release.job.index.function.CreateEsExportTar;

@RequiredArgsConstructor
public class EsExportTask extends GenericIndexTask {

  public static final String ES_EXPORT_DIR = "es_export";

  @NonNull
  private final String indexName;
  @NonNull
  private final DocumentType documentType;

  @Override
  public TaskType getType() {
    return TaskType.FILE_TYPE;
  }

  @Override
  public String getName() {
    return super.getName() + documentType.getName();
  }

  @Override
  public void execute(TaskContext taskContext) {
    prepareDirs(taskContext);
    readDocuments(taskContext)
        .coalesce(1)
        .foreachPartition(createExportTarFucntion(taskContext));

  }

  private CreateEsExportTar createExportTarFucntion(TaskContext taskContext) {
    return new CreateEsExportTar(
        indexName,
        taskContext.getJobContext().getWorkingDir(),
        documentType.getName(),
        getFileSystemSettings(taskContext));
  }

  private void prepareDirs(TaskContext taskContext) {
    val fileSystem = taskContext.getFileSystem();
    val workingDir = taskContext.getJobContext().getWorkingDir();
    val esExportsDir = new Path(workingDir, ES_EXPORT_DIR);
    HadoopUtils.mkdirs(fileSystem, esExportsDir);
  }

  private JavaRDD<Document> readDocuments(TaskContext taskContext) {
    return readUnpartitionedSequenceFileInput(taskContext, documentType.getOutputFileType())
        .map(new CreateDocument(documentType));
  }

  private static Map<String, String> getFileSystemSettings(TaskContext taskContext) {
    return Configurations.getSettings(taskContext.getFileSystem().getConf());
  }

}
