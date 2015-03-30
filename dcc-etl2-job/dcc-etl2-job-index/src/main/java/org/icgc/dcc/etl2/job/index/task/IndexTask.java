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
package org.icgc.dcc.etl2.job.index.task;

import lombok.val;

import org.apache.spark.api.java.JavaRDD;
import org.icgc.dcc.etl2.core.function.FilterFields;
import org.icgc.dcc.etl2.core.job.FileType;
import org.icgc.dcc.etl2.core.task.GenericTask;
import org.icgc.dcc.etl2.core.task.TaskContext;
import org.icgc.dcc.etl2.job.index.model.CollectionFields;
import org.icgc.dcc.etl2.job.index.model.DocumentType;
import org.icgc.dcc.etl2.job.index.util.CollectionFieldsFilterAdapter;

import scala.Tuple2;

import com.fasterxml.jackson.databind.node.ObjectNode;

public abstract class IndexTask extends GenericTask {

  protected final DocumentType type;

  public IndexTask(DocumentType type) {
    super(type.getName());
    this.type = type;
  }

  protected JavaRDD<ObjectNode> readReleases(TaskContext taskContext) {
    val fields = type.getFields().getReleaseFields();
    return filterFields(readInput(taskContext, FileType.RELEASE), fields);
  }

  protected JavaRDD<ObjectNode> readProjects(TaskContext taskContext) {
    val fields = type.getFields().getProjectFields();
    return filterFields(readInput(taskContext, FileType.PROJECT), fields);
  }

  protected JavaRDD<ObjectNode> readDonors(TaskContext taskContext) {
    val fields = type.getFields().getDonorFields();
    return filterFields(readInput(taskContext, FileType.DONOR), fields);
  }

  protected JavaRDD<ObjectNode> readGenes(TaskContext taskContext) {
    val fields = type.getFields().getGeneFields();
    return filterFields(readInput(taskContext, FileType.GENE), fields);
  }

  protected JavaRDD<ObjectNode> readGeneSets(TaskContext taskContext) {
    val fields = type.getFields().getGeneSetFields();
    return filterFields(readInput(taskContext, FileType.GENE_SET), fields);
  }

  protected JavaRDD<ObjectNode> readObservations(TaskContext taskContext) {
    val fields = type.getFields().getObservationFields();
    return filterFields(readInput(taskContext, FileType.OBSERVATION, "/*"), fields);
  }

  protected JavaRDD<ObjectNode> readMutations(TaskContext taskContext) {
    val fields = type.getFields().getMutationFields();
    return filterFields(readInput(taskContext, FileType.MUTATION), fields);
  }

  protected void writeOutput(JavaRDD<ObjectNode> output) {
    super.writeOutput(output, "indexer/" + type.getName());
  }

  protected static Tuple2<String, ObjectNode> pair(String id, ObjectNode row) {
    return new Tuple2<String, ObjectNode>(id, row);
  }

  protected static Tuple2<String, Iterable<ObjectNode>> pair(String id, Iterable<ObjectNode> rows) {
    return new Tuple2<String, Iterable<ObjectNode>>(id, rows);
  }

  private static JavaRDD<ObjectNode> filterFields(JavaRDD<ObjectNode> rdd, CollectionFields fields) {
    return rdd.map(new FilterFields(new CollectionFieldsFilterAdapter(fields)));
  }

}