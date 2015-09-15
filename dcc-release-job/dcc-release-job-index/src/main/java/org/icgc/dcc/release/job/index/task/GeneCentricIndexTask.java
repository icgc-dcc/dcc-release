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

import static org.icgc.dcc.release.job.index.model.CollectionFieldAccessors.getGeneId;
import static org.icgc.dcc.release.job.index.model.CollectionFieldAccessors.getObservationConsequenceGeneIds;

import java.util.List;
import java.util.Set;

import lombok.val;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.icgc.dcc.release.core.task.TaskContext;
import org.icgc.dcc.release.core.task.TaskType;
import org.icgc.dcc.release.job.index.function.GeneCentricRowTransform;
import org.icgc.dcc.release.job.index.model.DocumentType;

import scala.Tuple2;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class GeneCentricIndexTask extends IndexTask {

  public GeneCentricIndexTask() {
    super(DocumentType.GENE_CENTRIC_TYPE);
  }

  @Override
  public TaskType getType() {
    return TaskType.FILE_TYPE;
  }

  @Override
  public void execute(TaskContext taskContext) {
    val genes = readGenes(taskContext);
    val observations = readObservations(taskContext);

    // TODO: This should be configured to give optimum results
    // val splitSize = Long.toString(48 * 1024 * 1024);
    // conf.set("mapred.min.split.size", splitSize);
    // conf.set("mapred.max.split.size", splitSize);
    //
    // return ObjectNodeRDDs.combineObjectNodeFile(sparkContext, taskContext.getPath(inputFileType) + path, conf);
    //
    val output = transform(taskContext, genes, observations);
    writeOutput(output);
  }

  private JavaRDD<ObjectNode> transform(TaskContext taskContext,
      JavaRDD<ObjectNode> genes, JavaRDD<ObjectNode> observations) {
    val genePairs = genes.mapToPair(gene -> pair(getGeneId(gene), gene));

    val observationPairs =
        observations
            .flatMapToPair(new PairFlatMapFunction<ObjectNode, String, ObjectNode>() {

              @Override
              public Iterable<Tuple2<String, ObjectNode>> call(ObjectNode observation) throws Exception {
                Set<String> uniqueGeneIds = Sets.newHashSet(getObservationConsequenceGeneIds(observation));
                List<Tuple2<String, ObjectNode>> values = Lists.newArrayListWithCapacity(uniqueGeneIds.size());
                for (String observationGeneId : uniqueGeneIds) {
                  if (observationGeneId != null) {
                    values.add(pair(observationGeneId, observation));
                  }
                }

                return values;
              }
            })
            .groupBy(tuple -> tuple._1)
            .mapToPair(
                tuple -> {
                  String geneId = tuple._1;
                  Iterable<ObjectNode> geneObservations =
                      Iterables.transform(tuple._2, new Function<Tuple2<String, ObjectNode>, ObjectNode>() {

                        @Override
                        public ObjectNode apply(Tuple2<String, ObjectNode> t) {
                          return t._2;
                        }

                      });

                  return pair(geneId, geneObservations);
                });

    val geneObservationsPairs = genePairs.leftOuterJoin(observationPairs);
    val transformed = geneObservationsPairs.map(createTransform(taskContext));

    return transformed;
  }

  private GeneCentricRowTransform createTransform(TaskContext taskContext) {
    val collectionDir = taskContext.getJobContext().getWorkingDir();
    val fsUri = taskContext.getFileSystem().getUri();

    return new GeneCentricRowTransform(collectionDir, fsUri);
  }

}
