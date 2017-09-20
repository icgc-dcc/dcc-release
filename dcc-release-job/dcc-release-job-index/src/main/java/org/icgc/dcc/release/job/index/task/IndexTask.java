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

import org.apache.spark.api.java.JavaRDD;
import org.icgc.dcc.release.core.document.Document;
import org.icgc.dcc.release.core.document.DocumentType;
import org.icgc.dcc.release.core.task.Task;
import org.icgc.dcc.release.core.task.TaskContext;
import org.icgc.dcc.release.core.task.TaskPriority;
import org.icgc.dcc.release.core.task.TaskType;
import org.icgc.dcc.release.core.util.Configurations;
import org.icgc.dcc.release.job.index.function.CreateDocument;
import org.icgc.dcc.release.job.index.function.DocumentIndexer;



@RequiredArgsConstructor
public class IndexTask extends GenericIndexTask {

  private static final int PARTITION_SIZE_MB = 256;

  @NonNull
  private final String esUri;
  @NonNull
  private final String indexName;
  @NonNull
  private final DocumentType documentType;
  private final int bigDocumentThresholdMb;

  @Override
  public TaskType getType() {
    if (documentType.hasDefaultParallelism()) {
      return documentType.getOutputFileType().isPartitioned() ? TaskType.FILE_TYPE_PROJECT : TaskType.FILE_TYPE;
    }

    return TaskType.FILE_TYPE;
  }

  @Override
  public TaskPriority getPriority() {
    return documentType.getPriority();
  }

  @Override
  public String getName() {
    return Task.getName(super.getName(), documentType.getName());
  }

  @Override
  public void execute(TaskContext taskContext) {
    JavaRDD<Document> documents = readDocuments(taskContext);
    // If the documentType has parallelism set coalesce the number of mappers to that number.
    if (!documentType.hasDefaultParallelism()) {
      documents = documents.coalesce(documentType.getParallelism());
    }

    documents.foreachPartition(new DocumentIndexer(
        esUri,
        indexName,
        getFileSystemConfig(taskContext),
        bigDocumentThresholdMb,
        taskContext.getJobContext().getWorkingDir()));

  }

  private JavaRDD<Document> readDocuments(TaskContext taskContext) {
    return documentType.hasDefaultParallelism() ?
        readDocumnetInput(taskContext, documentType.getOutputFileType(), PARTITION_SIZE_MB) :
        readUnpartitionedSequenceFileInput(taskContext, documentType.getOutputFileType())
            .map(new CreateDocument(documentType));
  }

  private static Map<String, String> getFileSystemConfig(TaskContext taskContext) {
    return Configurations.getSettings(taskContext.getFileSystem().getConf());
  }

}
