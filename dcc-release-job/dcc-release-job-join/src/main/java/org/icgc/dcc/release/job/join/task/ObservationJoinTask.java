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
package org.icgc.dcc.release.job.join.task;

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.common.core.model.FieldNames.IdentifierFieldNames.SURROGATE_DONOR_ID;
import static org.icgc.dcc.common.core.model.FieldNames.NormalizerFieldNames.NORMALIZER_OBSERVATION_ID;
import static org.icgc.dcc.release.core.util.JavaRDDs.getPartitionsCount;
import static org.icgc.dcc.release.core.util.Keys.KEY_SEPARATOR;
import static org.icgc.dcc.release.job.join.utils.Tasks.getSampleSurrogateSampleIds;
import static org.icgc.dcc.release.job.join.utils.Tasks.resolveDonorSamples;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.icgc.dcc.common.core.model.Marking;
import org.icgc.dcc.release.core.function.KeyFields;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.task.GenericTask;
import org.icgc.dcc.release.core.task.TaskContext;
import org.icgc.dcc.release.core.util.CombineFunctions;
import org.icgc.dcc.release.core.util.Observations;
import org.icgc.dcc.release.core.util.SparkWorkaroundUtils;
import org.icgc.dcc.release.job.join.function.AggregateObservationConsequences;
import org.icgc.dcc.release.job.join.function.AggregateObservations;
import org.icgc.dcc.release.job.join.function.AggregatePrimarySecondary;
import org.icgc.dcc.release.job.join.function.CombineObservations;
import org.icgc.dcc.release.job.join.function.KeyAnalysisIdAnalyzedSampleIdField;
import org.icgc.dcc.release.job.join.function.KeyDonorMutataionId;
import org.icgc.dcc.release.job.join.model.DonorSample;

import scala.Tuple2;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;

@RequiredArgsConstructor
public class ObservationJoinTask extends GenericTask {

  private static final int DONOR_ID_INDEX = 0;
  private static final KeyFields PRIMARY_SECONDARY_KEY_FUNCTION = new KeyFields(NORMALIZER_OBSERVATION_ID);

  @NonNull
  private final Broadcast<Map<String, Map<String, DonorSample>>> donorSamplesBroadcast;
  @NonNull
  private final Broadcast<Map<String, Map<String, String>>> sampleSurrogateSampleIdsBroadcast;
  @NonNull
  private final List<String> controlledFields;

  @Override
  public void execute(TaskContext taskContext) {
    // Resolve loop-up info
    val donorSamples = resolveDonorSamples(taskContext, donorSamplesBroadcast);
    val sampleToSurrogageSampleId = getSampleSurrogateSampleIds(taskContext, sampleSurrogateSampleIdsBroadcast);

    // Prepare primaries
    val primarySecondaryKeyFunction = new KeyFields(NORMALIZER_OBSERVATION_ID);
    val primary = parseSsmP(taskContext)
        .mapToPair(primarySecondaryKeyFunction);

    val primaryPartitions = getPartitionsCount(primary);
    primary.persist(StorageLevel.MEMORY_ONLY_SER());

    // Aggregate consequences
    // TODO: confirm consequences are already unique
    val consequences = aggregateConsequences(taskContext, primaryPartitions);
    consequences.persist(StorageLevel.MEMORY_ONLY_SER());

    // Prepare meta
    val metaPairsBroadcast = resolveMeta(taskContext);

    // Join SSM
    val ssm = join(donorSamples, sampleToSurrogageSampleId, primary, consequences, metaPairsBroadcast);
    writeSsm(taskContext, ssm);

    // Join Observations
    val primaryOpen = filterControlledData(primary, controlledFields);
    val observations = join(donorSamples, sampleToSurrogageSampleId, primaryOpen, consequences, metaPairsBroadcast);
    writeObservation(taskContext, observations);

    primary.unpersist(false);
    consequences.unpersist(false);
  }

  private static JavaPairRDD<String, ObjectNode> filterControlledData(JavaPairRDD<String, ObjectNode> primary,
      List<String> controlledFields) {
    return primary
        .filter(filterControlledRecords())
        .mapValues(removeControlledFields(controlledFields));
  }

  private static Function<Tuple2<String, ObjectNode>, Boolean> filterControlledRecords() {
    return t -> {
      ObjectNode row = t._2;
      Optional<Marking> marking = Observations.getMarking(row);
      checkState(marking.isPresent(), "Failed to resolve marking from {}", row);

      return !marking.get().isControlled();
    };
  }

  private static JavaRDD<ObjectNode> join(
      Map<String, DonorSample> donorSamples,
      Map<String, String> sampleToSurrogageSampleId,
      JavaPairRDD<String, ObjectNode> primary,
      JavaPairRDD<String, Collection<ObjectNode>> consequences,
      Broadcast<Map<String, ObjectNode>> metaPairsBroadcast)
  {
    val ssm = primary.leftOuterJoin(consequences)
        .aggregateByKey(null, new AggregatePrimarySecondary(), combinePrimarySecondary())
        .mapToPair(new KeyDonorMutataionId(donorSamples))
        // create actual observation
        .aggregateByKey(null, new AggregateObservations(metaPairsBroadcast, donorSamples, sampleToSurrogageSampleId),
            new CombineObservations())
        .map(ObservationJoinTask::addSurrogateDonorId);

    return ssm;
  }

  private static ObjectNode addSurrogateDonorId(Tuple2<String, ObjectNode> tuple) {
    val occurrence = tuple._2;
    val donorIdMutationId = tuple._1;
    occurrence.put(SURROGATE_DONOR_ID, resolveDonorId(donorIdMutationId));

    return occurrence;
  }

  private Broadcast<Map<String, ObjectNode>> resolveMeta(TaskContext taskContext) {
    val metaPairs = parseSsmM(taskContext)
        .mapToPair(new KeyAnalysisIdAnalyzedSampleIdField())
        .collectAsMap();

    final Broadcast<Map<String, ObjectNode>> metaPairsBroadcast = taskContext
        .getSparkContext()
        .broadcast(SparkWorkaroundUtils.toHashMap(metaPairs));

    return metaPairsBroadcast;
  }

  private JavaPairRDD<String, Collection<ObjectNode>> aggregateConsequences(TaskContext taskContext,
      int primaryPartitions) {
    val zeroValue = Sets.<ObjectNode> newHashSet();
    val consequences = parseSsmS(taskContext)
        .mapToPair(PRIMARY_SECONDARY_KEY_FUNCTION)
        .aggregateByKey(zeroValue, primaryPartitions, new AggregateObservationConsequences(),
            CombineFunctions::combineCollections);

    return consequences;
  }

  private static Function2<ObjectNode, ObjectNode, ObjectNode> combinePrimarySecondary() {
    return (a, b) -> {
      throw new IllegalStateException("This function should never be called, as primary and secondary files should be"
          + " located on the same partition");
    };
  }

  private static Function<ObjectNode, ObjectNode> removeControlledFields(List<String> controlledFields) {
    return row -> {
      row.remove(controlledFields);

      return row;
    };
  }

  private void writeSsm(TaskContext taskContext, JavaRDD<ObjectNode> output) {
    val outputFileType = FileType.SSM;
    writeOutput(taskContext, output, outputFileType);
  }

  private void writeObservation(TaskContext taskContext, JavaRDD<ObjectNode> output) {
    val outputFileType = FileType.OBSERVATION;
    writeOutput(taskContext, output, outputFileType);
  }

  private JavaRDD<ObjectNode> parseSsmM(TaskContext taskContext) {
    return readInput(taskContext, FileType.SSM_M);
  }

  private JavaRDD<ObjectNode> parseSsmP(TaskContext taskContext) {
    return readInput(taskContext, FileType.SSM_P_MASKED_SURROGATE_KEY);
  }

  private JavaRDD<ObjectNode> parseSsmS(TaskContext taskContext) {
    return readInput(taskContext, FileType.SSM_S);
  }

  private static String resolveDonorId(String donorIdMutationId) {
    return donorIdMutationId.split(KEY_SEPARATOR)[DONOR_ID_INDEX];
  }

}