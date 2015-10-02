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

import static org.icgc.dcc.release.core.util.Tuples.tuple;
import static org.icgc.dcc.release.job.index.model.CollectionFieldAccessors.getGeneId;
import static org.icgc.dcc.release.job.index.util.GeneUtils.pivotGenes;
import static org.icgc.dcc.release.job.index.util.MutableMaps.toHashMap;

import java.util.Map;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.val;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.task.GenericTask;
import org.icgc.dcc.release.core.task.TaskContext;
import org.icgc.dcc.release.core.task.TaskType;

import com.fasterxml.jackson.databind.node.ObjectNode;

@NoArgsConstructor
public class ResolveGenesTask extends GenericTask {

  @Getter(lazy = true)
  private final Broadcast<Map<String, ObjectNode>> genesBroadcast = createBroadcast();
  private Map<String, ObjectNode> genesById;
  private JavaSparkContext sparkContext;

  @Override
  public TaskType getType() {
    return TaskType.FILE_TYPE;
  }

  @Override
  public void execute(TaskContext taskContext) {
    sparkContext = taskContext.getSparkContext();
    val genes = readGenes(taskContext)
        .mapToPair(gene -> tuple(getGeneId(gene), gene))
        .collectAsMap();
    genesById = toHashMap(genes);
  }

  private JavaRDD<ObjectNode> readGenes(TaskContext taskContext) {
    val genes = readInput(taskContext, FileType.GENE_SUMMARY);
    val geneSets = readInput(taskContext, FileType.GENE_SET_SUMMARY);

    return pivotGenes(genes, geneSets);
  }

  private Broadcast<Map<String, ObjectNode>> createBroadcast() {
    return sparkContext.broadcast(genesById);
  }

}
