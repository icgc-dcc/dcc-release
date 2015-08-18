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
package org.icgc.dcc.etl2.job.summarize.task;

import static org.icgc.dcc.common.core.model.FeatureTypes.FeatureType.SSM_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_ID;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_ID;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_DONOR_ID;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_TYPE;
import static org.icgc.dcc.etl2.core.function.Unwind.unwindToParent;
import static org.icgc.dcc.etl2.core.job.FileType.CLINICAL;
import static org.icgc.dcc.etl2.core.job.FileType.DONOR_GENE_OBSERVATION_SUMMARY;
import static org.icgc.dcc.etl2.core.job.FileType.OBSERVATION;
import static org.icgc.dcc.etl2.core.util.ObjectNodes.mergeObjects;
import static org.icgc.dcc.etl2.core.util.ObjectNodes.textValue;
import static org.icgc.dcc.etl2.core.util.Tasks.resolveProjectName;

import java.util.Map;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.icgc.dcc.etl2.core.function.KeyFields;
import org.icgc.dcc.etl2.core.function.RetainFields;
import org.icgc.dcc.etl2.core.task.GenericTask;
import org.icgc.dcc.etl2.core.task.TaskContext;
import org.icgc.dcc.etl2.job.summarize.function.CreateDonorGenesSummary;
import org.icgc.dcc.etl2.job.summarize.function.CreateDonorSummary;
import org.icgc.dcc.etl2.job.summarize.function.MergeDonorSummary;
import org.icgc.dcc.etl2.job.summarize.function.RetainObservationConsequenceFields;

import scala.Tuple2;

import com.fasterxml.jackson.databind.node.ObjectNode;

@RequiredArgsConstructor
public class DonorSummarizeTask extends GenericTask {

  @NonNull
  private final Broadcast<Map<String, Map<String, ObjectNode>>> projectDonorSummary;

  @Override
  public void execute(TaskContext taskContext) {
    val outputFileType = DONOR_GENE_OBSERVATION_SUMMARY;

    val projectName = resolveProjectName(taskContext);
    val summary = createDonorSummary(taskContext)
        .leftOuterJoin(summarizeDonorGenes(taskContext))
        .mapToPair(new MergeDonorSummary(projectDonorSummary, projectName));

    val output = readClinical(taskContext)
        .mapToPair(new KeyFields(DONOR_ID))
        .join(summary)
        .map(mergeDonorSummary());

    writeOutput(taskContext, output, outputFileType);
  }

  private static Function<Tuple2<String, Tuple2<ObjectNode, ObjectNode>>, ObjectNode> mergeDonorSummary() {
    return tuple -> {
      ObjectNode donor = tuple._2._1;
      ObjectNode donorSummary = tuple._2._2;
      return mergeObjects(donor, donorSummary);
    };
  }

  private JavaPairRDD<String, ObjectNode> createDonorSummary(TaskContext taskContext) {
    return readClinical(taskContext)
        .mapToPair(new KeyFields(DONOR_ID))
        .mapValues(new CreateDonorSummary());
  }

  private JavaPairRDD<String, ObjectNode> summarizeDonorGenes(TaskContext taskContext) {
    val input = readObservation(taskContext);

    return input
        .map(new RetainFields(OBSERVATION_DONOR_ID, OBSERVATION_CONSEQUENCES, OBSERVATION_TYPE, MUTATION_ID))
        .flatMap(unwindToParent(OBSERVATION_CONSEQUENCES))
        .filter(filterSsm())
        .map(new RetainObservationConsequenceFields())
        .mapToPair(new KeyFields(OBSERVATION_DONOR_ID))
        .distinct()
        .groupByKey()
        .mapToPair(new CreateDonorGenesSummary());
  }

  // DCC-1401: Only ssm for now
  private Function<ObjectNode, Boolean> filterSsm() {
    return row -> textValue(row, OBSERVATION_TYPE).equals(SSM_TYPE.getId());
  }

  private JavaRDD<ObjectNode> readObservation(TaskContext taskContext) {
    return readInput(taskContext, OBSERVATION);
  }

  private JavaRDD<ObjectNode> readClinical(TaskContext taskContext) {
    return readInput(taskContext, CLINICAL);
  }

}