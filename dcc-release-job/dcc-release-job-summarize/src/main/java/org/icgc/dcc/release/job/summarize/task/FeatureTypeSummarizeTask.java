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

import static com.google.common.collect.HashBasedTable.create;
import static com.google.common.collect.Maps.immutableEntry;
import static org.icgc.dcc.common.core.model.FieldNames.AVAILABLE_DATA_TYPES;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_ID;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames.OBSERVATION_TYPE;
import static org.icgc.dcc.release.core.function.PairFunctions.sum;
import static org.icgc.dcc.release.core.util.FeatureTypes.getFeatureTypes;
import static org.icgc.dcc.release.core.util.ObjectNodes.MAPPER;
import static org.icgc.dcc.release.core.util.ObjectNodes.mergeObjects;
import static org.icgc.dcc.release.core.util.Tasks.resolveProjectName;

import java.util.Map;
import java.util.Map.Entry;

import lombok.Getter;
import lombok.val;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.icgc.dcc.common.core.model.FeatureTypes.FeatureType;
import org.icgc.dcc.release.core.function.KeyFields;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.task.GenericTask;
import org.icgc.dcc.release.core.task.TaskContext;
import org.icgc.dcc.release.job.summarize.function.CreateFeatureTypeSummary;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;

public class FeatureTypeSummarizeTask extends GenericTask {

  @Getter(lazy = true)
  private final Broadcast<Map<String, Map<String, ObjectNode>>> projectDonorSummary = createBroadcastaVariable();
  private final Table<String, FeatureType, Map<String, ObjectNode>> projectFeatureTypeDonors = create();
  private JavaSparkContext sparkContext;

  @Override
  public void execute(TaskContext taskContext) {
    this.sparkContext = taskContext.getSparkContext();
    for (val featureType : getFeatureTypes()) {
      val projectName = resolveProjectName(taskContext);
      val featureTypeMapping = createMapping(taskContext, featureType);
      projectFeatureTypeDonors.put(projectName, featureType, featureTypeMapping);
    }
  }

  private Broadcast<Map<String, Map<String, ObjectNode>>> createBroadcastaVariable() {
    val projectDonorSummary = ImmutableMap.<String, Map<String, ObjectNode>> builder();
    for (val entry : projectFeatureTypeDonors.rowMap().entrySet()) {
      projectDonorSummary.put(mergeDonorSummaries(entry));
    }

    return sparkContext.broadcast(projectDonorSummary.build());
  }

  private static Entry<String, Map<String, ObjectNode>> mergeDonorSummaries(
      Entry<String, Map<FeatureType, Map<String, ObjectNode>>> projectFeatureTypeDonorSummaries) {
    val donorAggregatedSummaries = Maps.<String, ObjectNode> newHashMap();
    for (val featureTypeDonorSummaryEntry : projectFeatureTypeDonorSummaries.getValue().entrySet()) {
      val donorSummaries = featureTypeDonorSummaryEntry.getValue();
      for (val donorSummaryEntry : donorSummaries.entrySet()) {
        val donorId = donorSummaryEntry.getKey();
        val summary = donorAggregatedSummaries.get(donorId);
        val featureType = featureTypeDonorSummaryEntry.getKey();
        if (summary == null) {
          donorAggregatedSummaries.put(donorId, populateAvailableDataType(donorSummaryEntry.getValue(), featureType));
        } else {
          val aggregatedSummary = mergeObjects(summary, donorSummaryEntry.getValue());
          donorAggregatedSummaries.put(donorId, populateAvailableDataType(aggregatedSummary, featureType));
        }
      }
    }

    val projectName = projectFeatureTypeDonorSummaries.getKey();

    return immutableEntry(projectName, donorAggregatedSummaries);
  }

  private static ObjectNode populateAvailableDataType(ObjectNode donorSummary, FeatureType featureType) {
    val summary = donorSummary.with(DONOR_SUMMARY);
    val availableDataTypes = summary.withArray(AVAILABLE_DATA_TYPES);
    availableDataTypes.add(featureType.getId());

    return donorSummary;
  }

  private Map<String, ObjectNode> createMapping(TaskContext taskContext, FeatureType featureType) {
    val fileType = resolveInputFileType(featureType);

    // @formatter:off
    return 
        sum(
          readInput(taskContext, fileType)
          .mapToPair(new KeyFields(DONOR_ID, OBSERVATION_TYPE)))
        .mapToPair(new CreateFeatureTypeSummary())
        .aggregateByKey(MAPPER.createObjectNode(), aggregateFeatureType(), aggregateFeatureType())
        .collectAsMap();
    // @formatter:on
  }

  private Function2<ObjectNode, ObjectNode, ObjectNode> aggregateFeatureType() {
    return (agg, next) -> mergeObjects(agg, next);
  }

  private static FileType resolveInputFileType(FeatureType featureType) {
    return FileType.getFileType(featureType.getId());
  }

}
