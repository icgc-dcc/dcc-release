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
package org.icgc.dcc.release.job.document.task;

import static org.icgc.dcc.release.job.document.util.DocumentTypes.getFields;
import static org.icgc.dcc.release.job.document.util.GeneUtils.pivotGenes;
import lombok.val;

import org.apache.spark.api.java.JavaRDD;
import org.icgc.dcc.release.core.document.Document;
import org.icgc.dcc.release.core.document.DocumentType;
import org.icgc.dcc.release.core.function.FilterFields;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.task.GenericTask;
import org.icgc.dcc.release.core.task.TaskContext;
import org.icgc.dcc.release.core.util.JacksonFactory;
import org.icgc.dcc.release.job.document.model.CollectionFields;
import org.icgc.dcc.release.job.document.model.Occurrence;
import org.icgc.dcc.release.job.document.util.CollectionFieldsFilterAdapter;
import org.icgc.dcc.release.job.document.util.DocumentRdds;

import com.fasterxml.jackson.databind.node.ObjectNode;

public abstract class AbstractDocumentTask extends GenericTask {

  private static final FileType GENE_SET_INPUT_TYPE = FileType.GENE_SET_SUMMARY;

  protected final DocumentType type;

  public AbstractDocumentTask(DocumentType type) {
    super(type.getName());
    this.type = type;
  }

  protected JavaRDD<ObjectNode> readDiagrams(TaskContext taskContext) {
    val fields = getFields(type).getDiagramFields();
    return filterFields(readInput(taskContext, FileType.DIAGRAM), fields);
  }

  protected JavaRDD<ObjectNode> readDrugs(TaskContext taskContext) {
    val fields = getFields(type).getDrugFields();
    return filterFields(readInput(taskContext, FileType.DRUG), fields);
  }

  protected JavaRDD<ObjectNode> readReleases(TaskContext taskContext) {
    val fields = getFields(type).getReleaseFields();
    return filterFields(readInput(taskContext, FileType.RELEASE_SUMMARY), fields);
  }

  protected JavaRDD<ObjectNode> readProjects(TaskContext taskContext) {
    val fields = getFields(type).getProjectFields();
    return filterFields(readInput(taskContext, FileType.PROJECT_SUMMARY), fields);
  }

  protected JavaRDD<ObjectNode> readDonors(TaskContext taskContext) {
    val fields = getFields(type).getDonorFields();
    return filterFields(readInput(taskContext, FileType.DONOR_SUMMARY), fields);
  }

  protected JavaRDD<ObjectNode> readGenesPivoted(TaskContext taskContext) {
    val fields = getFields(type).getGeneFields();
    val genes = filterFields(readInput(taskContext, FileType.GENE_SUMMARY), fields);
    val geneSets = readInput(taskContext, GENE_SET_INPUT_TYPE);

    return pivotGenes(genes, geneSets);
  }

  protected JavaRDD<ObjectNode> readGenes(TaskContext taskContext) {
    val fields = getFields(type).getGeneFields();
    return filterFields(readInput(taskContext, FileType.GENE_SUMMARY), fields);
  }

  protected JavaRDD<ObjectNode> readGeneSets(TaskContext taskContext) {
    val fields = getFields(type).getGeneSetFields();
    return filterFields(readInput(taskContext, GENE_SET_INPUT_TYPE), fields);
  }

  protected JavaRDD<ObjectNode> readObservations(TaskContext taskContext) {
    val fields = getFields(type).getObservationFields();
    return filterFields(readInput(taskContext, FileType.OBSERVATION_FI), fields);
  }

  protected JavaRDD<ObjectNode> readMutations(TaskContext taskContext) {
    val fields = getFields(type).getMutationFields();
    return filterFields(readInput(taskContext, FileType.MUTATION), fields);
  }

  protected void writeDocOutput(TaskContext taskContext, JavaRDD<Document> processed) {
    val outputPath = taskContext.getPath(type.getOutputFileType());
    if (taskContext.isCompressOutput()) {
      DocumentRdds.saveAsSequenceObjectNodeFile(processed, outputPath);
    } else {
      DocumentRdds.saveAsTextObjectNodeFile(processed, outputPath);
    }
  }

  protected JavaRDD<Occurrence> readOccurrences(TaskContext taskContext) {
    return readObservations(taskContext)
        .map(row -> JacksonFactory.MAPPER.treeToValue(row, Occurrence.class));
  }

  private static JavaRDD<ObjectNode> filterFields(JavaRDD<ObjectNode> rdd, CollectionFields fields) {
    return rdd.map(new FilterFields(new CollectionFieldsFilterAdapter(fields)));
  }

}