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
package org.icgc.dcc.release.job.index.task;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;

import java.util.UUID;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import org.apache.spark.api.java.function.Function;
import org.icgc.dcc.release.core.document.Document;
import org.icgc.dcc.release.core.document.DocumentType;
import org.icgc.dcc.release.core.task.GenericSerializableTask;
import org.icgc.dcc.release.core.task.Task;
import org.icgc.dcc.release.core.task.TaskContext;
import org.icgc.dcc.release.core.task.TaskType;
import org.icgc.dcc.release.core.util.ObjectNodes;

import com.fasterxml.jackson.databind.node.ObjectNode;

@RequiredArgsConstructor
public class IndexTask extends GenericSerializableTask  {

  private static final long serialVersionUID = 5207503787859232749L;
  
  @NonNull
  private final String esUri;
  @NonNull
  private final String indexName;
  @NonNull
  private final DocumentType documentType;

  @Override
  public TaskType getType() {
    return documentType.getOutputFileType().isPartitioned() ? TaskType.FILE_TYPE_PROJECT : TaskType.FILE_TYPE;
  }

  @Override
  public String getName() {
    return Task.getName(super.getName(), documentType.getName());
  }

  @Override
  public void execute(TaskContext taskContext) {
    readInput(taskContext, documentType.getOutputFileType())
        .map(createDocument())
        .mapPartitions(new DocumentIndexer(esUri, indexName, documentType))
        .count();
  }

  private Function<ObjectNode, Document> createDocument() {
    return o -> {
      String idFieldName = documentType.getPrimaryKey();

      String id = documentType == DocumentType.OBSERVATION_CENTRIC_TYPE ?
          UUID.randomUUID().toString()
          : ObjectNodes.textValue(o, idFieldName);
      checkState(!isNullOrEmpty(id), "Document ID can't be null or empty. {}", o);

      return new Document(documentType, id, o);
    };
  }

}
