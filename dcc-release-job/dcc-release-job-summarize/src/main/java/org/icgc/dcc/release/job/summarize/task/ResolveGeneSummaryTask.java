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
package org.icgc.dcc.release.job.summarize.task;

import static com.google.common.collect.Lists.newCopyOnWriteArrayList;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_ID;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_DONOR_ID;
import static org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames.OBSERVATION_TYPE;
import static org.icgc.dcc.release.core.function.Unwind.unwindToParent;
import static org.icgc.dcc.release.core.util.FieldNames.SummarizeFieldNames.FAKE_GENE_ID;
import static org.icgc.dcc.release.core.util.ObjectNodes.textValue;
import static org.icgc.dcc.release.core.util.Tasks.resolveProjectName;
import static org.icgc.dcc.release.core.util.Tuples.tuple;

import java.util.List;

import lombok.Getter;
import lombok.val;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.icgc.dcc.release.core.function.KeyFieldsFunction;
import org.icgc.dcc.release.core.function.RetainFields;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.task.GenericTask;
import org.icgc.dcc.release.core.task.TaskContext;
import org.icgc.dcc.release.job.summarize.function.AggregateGeneStats;
import org.icgc.dcc.release.job.summarize.function.CombineGeneStats;
import org.icgc.dcc.release.job.summarize.function.ConvertToTempGeneSummaryObject;
import org.icgc.dcc.release.job.summarize.function.MergeGeneSummaries;

import scala.Tuple2;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class ResolveGeneSummaryTask extends GenericTask {

  private static final String MATCH_MUTATION_ID_REGEX = "[\\d\\w]*#";
  @Getter(lazy = true)
  private final Broadcast<JavaPairRDD<String, ObjectNode>> geneDonorTypeCounts = createBroadcastVariable();
  private final List<JavaPairRDD<String, ObjectNode>> geneStats = newCopyOnWriteArrayList();
  private JavaSparkContext sparkContext;

  @Override
  public void execute(TaskContext taskContext) {
    this.sparkContext = taskContext.getSparkContext();
    // Order is important, as MUTATION_ID is removed later
    val keyGeneFieldsFunction = new KeyFieldsFunction<Integer>(o -> 1, MUTATION_ID, OBSERVATION_DONOR_ID,
        OBSERVATION_TYPE, GENE_ID);
    val projectName = resolveProjectName(taskContext);
    val geneStats = readObservations(taskContext)
        .filter(filterFakeGeneIds())
        .mapToPair(keyGeneFieldsFunction)
        .distinct()
        .mapToPair(ResolveGeneSummaryTask::removeMutation)
        .reduceByKey((a, b) -> a + b)
        .mapToPair(new ConvertToTempGeneSummaryObject(projectName))
        .aggregateByKey(null, new AggregateGeneStats(), new CombineGeneStats());

    this.geneStats.add(geneStats);
  }

  private Broadcast<JavaPairRDD<String, ObjectNode>> createBroadcastVariable() {
    return sparkContext.broadcast(joinGeneStats());
  }

  private JavaPairRDD<String, ObjectNode> joinGeneStats() {
    // ETL runs on at least one project
    JavaPairRDD<String, ObjectNode> resultRdd = geneStats.get(0);
    for (int i = 1; i < geneStats.size(); i++) {
      val currentRdd = geneStats.get(i);
      resultRdd = resultRdd
          .fullOuterJoin(currentRdd)
          .mapToPair(new MergeGeneSummaries());
    }

    return resultRdd;
  }

  private static Tuple2<String, Integer> removeMutation(Tuple2<String, Integer> tuple) {
    val oldKey = tuple._1;
    val newKey = oldKey.replaceFirst(MATCH_MUTATION_ID_REGEX, "");

    return tuple(newKey, tuple._2);
  }

  private static Function<ObjectNode, Boolean> filterFakeGeneIds() {
    return o -> !textValue(o, GENE_ID).equals(FAKE_GENE_ID);
  }

  private JavaRDD<ObjectNode> readObservations(TaskContext taskContext) {
    val retainGeneFields = new RetainGeneFields(OBSERVATION_DONOR_ID, OBSERVATION_TYPE, OBSERVATION_CONSEQUENCES,
        GENE_ID, MUTATION_ID);
    return readInput(taskContext, FileType.OBSERVATION_FI)
        .map(new RetainFields(OBSERVATION_DONOR_ID, OBSERVATION_TYPE, OBSERVATION_CONSEQUENCES, MUTATION_ID))
        .flatMap(unwindToParent(OBSERVATION_CONSEQUENCES))
        .map(retainGeneFields);
  }

  private static class RetainGeneFields extends RetainFields {

    public RetainGeneFields(String... fieldNames) {
      super(fieldNames);
    }

    @Override
    public ObjectNode call(ObjectNode row) {
      val result = super.call(row);
      val geneId = result.path(GENE_ID);
      if (geneId.isMissingNode() || geneId.isNull()) {
        result.put(GENE_ID, FAKE_GENE_ID);
      }

      return result;
    }
  }

}
