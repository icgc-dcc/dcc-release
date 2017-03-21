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
import static org.icgc.dcc.release.core.util.Partitions.getPartitionsCount;
import static org.icgc.dcc.release.core.util.Tuples.tuple;
import static org.icgc.dcc.release.job.join.utils.Tasks.getSampleSurrogateSampleIds;
import static org.icgc.dcc.release.job.join.utils.Tasks.resolveDonorSamples;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.icgc.dcc.common.core.model.Marking;
import org.icgc.dcc.common.core.util.Separators;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.task.GenericTask;
import org.icgc.dcc.release.core.task.TaskContext;
import org.icgc.dcc.release.core.util.CombineFunctions;
import org.icgc.dcc.release.core.util.JacksonFactory;
import org.icgc.dcc.release.core.util.Keys;
import org.icgc.dcc.release.core.util.SparkWorkaroundUtils;
import org.icgc.dcc.release.job.join.function.AggregateObservationConsequences;
import org.icgc.dcc.release.job.join.function.AggregateOccurrences;
import org.icgc.dcc.release.job.join.function.CreateOccurrence;
import org.icgc.dcc.release.job.join.function.KeyDonorMutataionId;
import org.icgc.dcc.release.job.join.model.DonorSample;
import org.icgc.dcc.release.job.join.model.SsmMetaFeatureType;
import org.icgc.dcc.release.job.join.model.SsmOccurrence;
import org.icgc.dcc.release.job.join.model.SsmOccurrence.Consequence;
import org.icgc.dcc.release.job.join.model.SsmPrimaryFeatureType;

import scala.Tuple2;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;

@RequiredArgsConstructor
public class ObservationJoinTask extends GenericTask {

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
    val primary = parseSsmP(taskContext)
        .mapToPair(o -> tuple(o.getObservation_id(), o));

    val poo = primary.collectAsMap();

    val primaryPartitions = getPartitionsCount(primary);
    primary.persist(StorageLevel.MEMORY_ONLY_SER());

    // Aggregate consequences
    val consequences = aggregateConsequences(taskContext, primaryPartitions);
    consequences.persist(StorageLevel.MEMORY_ONLY_SER());

    // Prepare meta
    val metaPairsBroadcast = resolveMeta(taskContext);

    // Join SSM
    val ssm = join(donorSamples, sampleToSurrogageSampleId, primary, consequences, metaPairsBroadcast);
    val doodoo = ssm.collect();
    writeSsm(taskContext, ssm);

    // Join Observations
    val primaryOpen = filterControlledData(primary, controlledFields);
    val observations = join(donorSamples, sampleToSurrogageSampleId, primaryOpen, consequences, metaPairsBroadcast);
    writeObservation(taskContext, observations);

    primary.unpersist(false);
    consequences.unpersist(false);
  }

  private static JavaPairRDD<String, SsmPrimaryFeatureType> filterControlledData(
      JavaPairRDD<String, SsmPrimaryFeatureType> primary,
      List<String> controlledFields) {
    return primary
        .filter(filterControlledRecords())
        .mapValues(removeControlledFields(controlledFields));
  }

  private static Function<Tuple2<String, SsmPrimaryFeatureType>, Boolean> filterControlledRecords() {
    return t -> {
      SsmPrimaryFeatureType row = t._2;
      Optional<Marking> marking = Marking.from(row.getMarking());
      checkState(marking.isPresent(), "Failed to resolve marking from %s", row);

      return !marking.get().isControlled();
    };
  }

  private static JavaRDD<SsmOccurrence> join(
      Map<String, DonorSample> donorSamples,
      Map<String, String> sampleToSurrogageSampleId,
      JavaPairRDD<String, SsmPrimaryFeatureType> primary,
      JavaPairRDD<String, Collection<Consequence>> consequences,
      Broadcast<Map<String, SsmMetaFeatureType>> metaPairsBroadcast)
  {
    SsmOccurrence zeroValue = null;
    val createOccurrences = new CreateOccurrence(metaPairsBroadcast, donorSamples, sampleToSurrogageSampleId);

    val occurrences = primary
        .leftOuterJoin(consequences)
        .aggregateByKey(zeroValue, createOccurrences, combinePrimarySecondary())
        .mapToPair(new KeyDonorMutataionId(donorSamples));

    // Merge occurrences
    val aggregateFunction = new AggregateOccurrences();

    return occurrences
        .aggregateByKey(zeroValue, aggregateFunction, aggregateFunction)
        .values();
  }

  private Broadcast<Map<String, SsmMetaFeatureType>> resolveMeta(TaskContext taskContext) {
    // Meta files are small. Put them in memory and distribute to workers.
    val metaPairs = parseSsmM(taskContext)
        .mapToPair(keyMeta())
        .collectAsMap();

    final Broadcast<Map<String, SsmMetaFeatureType>> metaPairsBroadcast = taskContext
        .getSparkContext()
        .broadcast(SparkWorkaroundUtils.toHashMap(metaPairs));

    return metaPairsBroadcast;
  }

  private JavaPairRDD<String, Collection<Consequence>> aggregateConsequences(TaskContext taskContext,
      int primaryPartitions) {
    // Submitted(generated) consequences(ssm_s, sgv_s, cmsm_s and stsm_s files) might not be unique.
    // After speaking with our bioinformatician it was decided to enforce their uniqueness when they are joined.
    val zeroValue = Sets.<Consequence> newHashSet();

    return parseSsmS(taskContext)
        .mapToPair(o -> tuple(o.getObservation_id(), o))
        .aggregateByKey(zeroValue, primaryPartitions, new AggregateObservationConsequences(),
            CombineFunctions::combineCollections);
  }

  private static Function2<SsmOccurrence, SsmOccurrence, SsmOccurrence> combinePrimarySecondary() {
    return (a, b) -> {
      throw new IllegalStateException("This function should never be called, as primary and secondary files should be"
          + " located on the same partition");
    };
  }

  private static Function<SsmPrimaryFeatureType, SsmPrimaryFeatureType> removeControlledFields(
      List<String> controlledFields) {
    return row -> {
      for (String field : controlledFields) {
        Class<?> clazz = getParamType(row, field);
        unsetField(row, field, clazz);
      }

      return row;
    };
  }

  @SneakyThrows
  private static void unsetField(SsmPrimaryFeatureType primary, String field, Class<?> clazz) {
    val setter = getMethod(primary, field, "set", clazz);
    setter.invoke(primary, new Object[] { null });
  }

  @SneakyThrows
  private static Class<?> getParamType(SsmPrimaryFeatureType primary, String field) {
    val getter = getMethod(primary, field, "get");

    return getter.getReturnType();
  }

  private static Method getMethod(SsmPrimaryFeatureType primary, String field, String prefix, Class<?>... params)
      throws NoSuchMethodException {
    val methodName = resolveMethodName(field, prefix);
    val method = primary.getClass().getMethod(methodName, params);

    return method;
  }

  private static String resolveMethodName(String field, String prefix) {
    val methodName = field.startsWith(Separators.UNDERSCORE) ? field : capitalizeFirstLetter(field);

    return prefix + methodName;
  }

  private static String capitalizeFirstLetter(String original) {
    if (original == null || original.length() == 0) {
      return original;
    }

    return original.substring(0, 1).toUpperCase() + original.substring(1);
  }

  private static PairFunction<SsmMetaFeatureType, String, SsmMetaFeatureType> keyMeta() {
    return meta -> {
      String key = Keys.getKey(meta.getAnalysis_id(), meta.getAnalyzed_sample_id());

      return tuple(key, meta);
    };
  }

  private void writeSsm(TaskContext taskContext, JavaRDD<SsmOccurrence> output) {
    val outputFileType = FileType.SSM;
    writeOutput(taskContext, output, outputFileType, SsmOccurrence.class);
  }

  private void writeObservation(TaskContext taskContext, JavaRDD<SsmOccurrence> output) {
    val outputFileType = FileType.OBSERVATION;
    writeOutput(taskContext, output, outputFileType, SsmOccurrence.class);
  }

  private JavaRDD<SsmMetaFeatureType> parseSsmM(TaskContext taskContext) {
    return readInput(taskContext, FileType.SSM_M)
        .map(row -> JacksonFactory.MAPPER.treeToValue(row, SsmMetaFeatureType.class));
  }

  private JavaRDD<SsmPrimaryFeatureType> parseSsmP(TaskContext taskContext) {
    return readInput(taskContext, FileType.SSM_P_MASKED_SURROGATE_KEY)
        .map(row -> JacksonFactory.MAPPER.treeToValue(row, SsmPrimaryFeatureType.class));
  }

  private JavaRDD<Consequence> parseSsmS(TaskContext taskContext) {
    return readInput(taskContext, FileType.SSM_S)
        .map(row -> JacksonFactory.MAPPER.treeToValue(row, Consequence.class));
  }

}