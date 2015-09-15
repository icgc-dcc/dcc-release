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

import static com.google.common.collect.Lists.newArrayList;
import static org.icgc.dcc.common.core.model.FieldNames.AVAILABLE_DATA_TYPES;
import static org.icgc.dcc.common.core.model.FieldNames.AVAILABLE_EXPERIMENTAL_ANALYSIS_PERFORMED;
import static org.icgc.dcc.common.core.model.FieldNames.EXPERIMENTAL_ANALYSIS_PERFORMED_SAMPLE_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.getTestedTypeCountFieldName;
import static org.icgc.dcc.common.core.util.Jackson.asArrayNode;
import static org.icgc.dcc.common.core.util.Jackson.from;
import static org.icgc.dcc.common.core.util.Jackson.to;
import static org.icgc.dcc.etl2.core.util.FeatureTypes.getFeatureTypes;
import static org.icgc.dcc.etl2.core.util.ObjectNodes.textValue;
import static org.icgc.dcc.etl2.core.util.Tasks.resolveProjectName;
import static org.icgc.dcc.etl2.job.summarize.util.Projects.createDefaultProjectSummary;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import lombok.Getter;
import lombok.val;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.icgc.dcc.common.core.model.FeatureTypes.FeatureType;
import org.icgc.dcc.common.core.model.FieldNames;
import org.icgc.dcc.etl2.core.function.PullUpField;
import org.icgc.dcc.etl2.core.function.RetainFields;
import org.icgc.dcc.etl2.core.function.Unwind;
import org.icgc.dcc.etl2.core.job.FileType;
import org.icgc.dcc.etl2.core.task.GenericTask;
import org.icgc.dcc.etl2.core.task.TaskContext;
import org.icgc.dcc.etl2.core.util.Tuples;

import scala.Tuple2;

import com.clearspring.analytics.util.Lists;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

public class ResolveProjectSummaryTask extends GenericTask {

  @Getter(lazy = true)
  private final Broadcast<Map<String, ObjectNode>> projectSummaryBroadcast = createBroadcastVariable();
  private final Map<String, ObjectNode> projectSummaries = Maps.newHashMap();
  private JavaSparkContext sparkContext;

  @Override
  public void execute(TaskContext taskContext) {
    sparkContext = taskContext.getSparkContext();
    val projectSummary = createDefaultProjectSummary();
    val donorSummaries = readDonorSummary(taskContext);

    donorSummaries.cache();
    summarizeAvailableTypes(donorSummaries, projectSummary);
    summarizeTotalDonorCounts(donorSummaries, projectSummary);
    summarizeTotalLiveDonorCounts(donorSummaries, projectSummary);
    summarizeTestedTypeCounts(donorSummaries, projectSummary);
    summarizeRepositories(donorSummaries, projectSummary);
    summarizeLibraryStrategyCounts(donorSummaries, projectSummary);
    donorSummaries.unpersist();

    summarizeSpecimenSampleCounts(taskContext, projectSummary);

    val projectName = resolveProjectName(taskContext);
    this.projectSummaries.put(projectName, projectSummary);
  }

  private Broadcast<Map<String, ObjectNode>> createBroadcastVariable() {
    return sparkContext.broadcast(projectSummaries);
  }

  private void summarizeTestedTypeCounts(JavaRDD<ObjectNode> donorSummaries, ObjectNode projectSummary) {
    for (val featureType : getFeatureTypes()) {
      val featureTypeCount = donorSummaries
          .filter(filterFeatureType(featureType))
          .count();
      projectSummary.put(getTestedTypeCountFieldName(featureType), featureTypeCount);
    }
  }

  private Function<ObjectNode, Boolean> filterFeatureType(FeatureType featureType) {
    val summaryFieldName = featureType.getSummaryFieldName();
    featureType.isCountSummary();

    return o -> {
      JsonNode summaryField = o.get(summaryFieldName);
      return featureType.isCountSummary() ?
          summaryField.asLong() > 0L :
          summaryField.asBoolean();
    };
  }

  private void summarizeLibraryStrategyCounts(JavaRDD<ObjectNode> donorSummaries, ObjectNode projectSummary) {
    val donorLibStrategies = donorSummaries
        .map(new RetainFields(FieldNames.DONOR_SUMMARY_EXPERIMENTAL_ANALYSIS_SAMPLE_COUNTS))
        .map(new PullUpField(FieldNames.DONOR_SUMMARY_EXPERIMENTAL_ANALYSIS_SAMPLE_COUNTS));
    donorLibStrategies.cache();

    val uniqueLibStrategies = donorLibStrategies
        .flatMap(o -> newArrayList(o.fieldNames()))
        .distinct()
        .collect();

    val donorLibraryStrategyCounts = ImmutableMap.<String, Long> builder();
    for (val libStrategy : uniqueLibStrategies) {
      val donorLibStrategy = donorLibStrategies.filter(o -> !o.path(libStrategy).isMissingNode());
      val donorCount = donorLibStrategy.count();
      donorLibraryStrategyCounts.put(libStrategy, donorCount);
    }

    val sampleLibraryStrategyCounts = donorLibStrategies
        .flatMapToPair(flattenLibStrategyToPair())
        .reduceByKey((a, b) -> a + b)
        .collectAsMap();

    projectSummary.putPOJO(AVAILABLE_EXPERIMENTAL_ANALYSIS_PERFORMED, to(uniqueLibStrategies));
    projectSummary.put(EXPERIMENTAL_ANALYSIS_PERFORMED_SAMPLE_COUNT, to(sampleLibraryStrategyCounts));

    donorLibStrategies.unpersist();
  }

  private PairFlatMapFunction<ObjectNode, String, Integer> flattenLibStrategyToPair() {
    return o -> {
      List<Tuple2<String, Integer>> result = Lists.<Tuple2<String, Integer>> newArrayList();
      Iterator<Map.Entry<String, JsonNode>> iterator = o.fields();
      while (iterator.hasNext()) {
        Entry<String, JsonNode> entry = iterator.next();
        result.add(Tuples.tuple(entry.getKey(), entry.getValue().asInt()));
      }
      return result;
    };
  }

  private void summarizeRepositories(JavaRDD<ObjectNode> donorSummaries, ObjectNode projectSummary) {
    val donorRepositories = donorSummaries
        .flatMap(unwindArray(FieldNames.DONOR_SUMMARY_REPOSITORY))
        .distinct()
        .collect();

    projectSummary.putPOJO(FieldNames.PROJECT_SUMMARY_REPOSITORY, to(donorRepositories));
  }

  private void summarizeSpecimenSampleCounts(TaskContext taskContext, ObjectNode projectSummary) {
    val specimens = readDonors(taskContext)
        .flatMap(Unwind.unwind(FieldNames.DONOR_SPECIMEN))
        .map(new RetainFields(FieldNames.DONOR_SPECIMEN_ID, FieldNames.DONOR_SAMPLE));
    specimens.cache();
    val specimenCount = specimens.count();
    val samples = specimens
        .flatMap(Unwind.unwind(FieldNames.DONOR_SAMPLE))
        .map(new RetainFields(FieldNames.DONOR_SAMPLE_ID));
    val samplesCount = samples.count();
    specimens.unpersist();

    projectSummary.put(FieldNames.TOTAL_SPECIMEN_COUNT, specimenCount);
    projectSummary.put(FieldNames.TOTAL_SAMPLE_COUNT, samplesCount);
  }

  private void summarizeTotalLiveDonorCounts(JavaRDD<ObjectNode> donorSummaries, ObjectNode projectSummary) {
    val totalLiveDonors = donorSummaries
        .filter(o -> textValue(o, FieldNames.DONOR_SUMMARY_STATE).equals("live"))
        .count();

    projectSummary.put(FieldNames.TOTAL_LIVE_DONOR_COUNT, totalLiveDonors);
    val state = totalLiveDonors > 0 ? "live" : "pending";
    projectSummary.put(FieldNames.PROJECT_SUMMARY_STATE, state);
  }

  private void summarizeTotalDonorCounts(JavaRDD<ObjectNode> donorSummaries, ObjectNode projectSummary) {
    val donorsCount = donorSummaries.count();
    projectSummary.put(FieldNames.TOTAL_DONOR_COUNT, donorsCount);
  }

  private void summarizeAvailableTypes(JavaRDD<ObjectNode> donorSummaries, ObjectNode projectSummary) {
    val availableDataTypes = donorSummaries
        .flatMap(unwindArray(AVAILABLE_DATA_TYPES))
        .distinct()
        .collect();

    projectSummary.putPOJO(AVAILABLE_DATA_TYPES, to(availableDataTypes));
  }

  private FlatMapFunction<ObjectNode, String> unwindArray(String unwindField) {

    return o -> {
      JsonNode availableDataType = o.get(unwindField);

      return from(asArrayNode(availableDataType), String.class);
    };
  }

  private JavaRDD<ObjectNode> readDonorSummary(TaskContext taskContext) {
    return readInput(taskContext, FileType.DONOR_GENE_OBSERVATION_SUMMARY)
        .map(new RetainFields(FieldNames.DONOR_SUMMARY))
        .map(new PullUpField(FieldNames.DONOR_SUMMARY));
  }

  private JavaRDD<ObjectNode> readDonors(TaskContext taskContext) {
    return readInput(taskContext, FileType.DONOR_GENE_OBSERVATION_SUMMARY);
  }

}
