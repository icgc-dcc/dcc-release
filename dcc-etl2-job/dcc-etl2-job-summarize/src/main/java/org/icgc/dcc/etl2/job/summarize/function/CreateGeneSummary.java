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
package org.icgc.dcc.etl2.job.summarize.function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.get;
import static com.google.common.collect.Iterables.isEmpty;
import static com.google.common.collect.Sets.newTreeSet;
import static org.icgc.dcc.common.core.model.FieldNames.AFFECTED_DONOR_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.AVAILABLE_DATA_TYPES;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_ID;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_DONORS;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_DONOR_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_PROJECTS;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_PROJECT_PROJECT_ID;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_PROJECT_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.PROJECT_ID;
import static org.icgc.dcc.common.core.util.Jackson.asObjectNode;
import static org.icgc.dcc.etl2.core.util.FeatureTypes.createFeatureTypeSummaryValue;
import static org.icgc.dcc.etl2.core.util.ObjectNodes.createArray;
import static org.icgc.dcc.etl2.core.util.ObjectNodes.createObject;
import static org.icgc.dcc.etl2.core.util.ObjectNodes.mergeObjects;
import static org.icgc.dcc.etl2.core.util.ObjectNodes.textValue;

import java.util.Map;

import lombok.val;

import org.apache.spark.api.java.function.Function;
import org.icgc.dcc.common.core.model.FeatureTypes.FeatureType;

import com.clearspring.analytics.util.Lists;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

public final class CreateGeneSummary implements Function<ObjectNode, ObjectNode> {

  @Override
  public ObjectNode call(ObjectNode geneStats) throws Exception {
    // geneStats - {donors:[{_donor_id:D1, ssm:2, _project_id:P1},{...}]}
    val donorsSourceArray = geneStats.withArray(GENE_DONORS);
    val donorIdGeneStats = createDonorIdDonorMultimap(donorsSourceArray);

    val projectAvailableTypes = ArrayListMultimap.<String, FeatureType> create();
    val projectDonorsCount = Maps.<String, Integer> newHashMap();

    val geneSummary = createObject();
    val geneDonors = geneSummary.withArray(GENE_DONORS);
    for (val geneStatsEntry : donorIdGeneStats.asMap().entrySet()) {
      // projectId must be resolved before createDonor() called, as it removes projectId from geneStat objects
      val projectId = extractProjectId(geneStatsEntry.getValue());
      val donor = createDonor(geneStatsEntry.getValue());
      geneDonors.add(donor);

      incrementProjectDonors(projectDonorsCount, projectId);
      projectAvailableTypes.putAll(projectId, getDonorFeatureTypes(donor));
    }

    val projectsArray = geneSummary.withArray(GENE_PROJECTS);
    projectsArray.addAll(createProjectArray(projectDonorsCount, projectAvailableTypes));

    return geneSummary;
  }

  private ArrayNode createProjectArray(Map<String, Integer> projectToDonors,
      ArrayListMultimap<String, FeatureType> projectAvailableTypes) {
    val projectArray = createArray();
    for (val projectId : projectToDonors.keySet()) {
      projectArray.add(
          createProjectEntry(
              projectId,
              projectToDonors,
              projectAvailableTypes));
    }

    return projectArray;
  }

  /**
   * @return <pre>
   * {
   *   "_project_id": "P1",
   *   "_summary": {
   *     "_affected_donor_count": 4,
   *     "_available_data_type": [
   *       "ssm",
   *       "sgv"
   *     ]
   *   }
   * }
   * </pre>
   */
  private static ObjectNode createProjectEntry(
      String projectId,
      Map<String, Integer> projectToDonors,
      Multimap<String, FeatureType> projectAvailableTypes) {
    val projectEntry = createObject();
    projectEntry.put(GENE_PROJECT_PROJECT_ID, projectId);

    val summaryEntry = projectEntry.with(GENE_PROJECT_SUMMARY);
    val donorCount = projectToDonors.get(projectId);
    summaryEntry.put(AFFECTED_DONOR_COUNT, donorCount);

    val availableTypes = summaryEntry.withArray(AVAILABLE_DATA_TYPES);
    for (val type : newTreeSet(projectAvailableTypes.get(projectId))) {
      availableTypes.add(type.getId());
    }

    return projectEntry;
  }

  private static Iterable<FeatureType> getDonorFeatureTypes(ObjectNode donor) {
    val summary = donor.with(GENE_DONOR_SUMMARY);
    val donorFeatureTypes = Lists.<FeatureType> newArrayList();
    for (val featureType : FeatureType.values()) {
      if (isIncludeFeatureType(featureType, summary)) {
        donorFeatureTypes.add(featureType);
      }
    }

    return donorFeatureTypes;
  }

  private static boolean isIncludeFeatureType(FeatureType featureType, ObjectNode summary) {
    val value = summary.get(featureType.getSummaryFieldName());
    if (featureType.isCountSummary()) {
      return value.asInt() > 0;
    } else {
      return value.asBoolean();
    }
  }

  private ObjectNode createDonor(Iterable<ObjectNode> geneStats) {
    val donor = createObject();
    val donorId = extractDonorId(geneStats);
    donor.put(DONOR_ID, donorId);
    donor.put(GENE_DONOR_SUMMARY, createGeneDonorSummary(geneStats));

    return donor;
  }

  private ObjectNode createGeneDonorSummary(Iterable<ObjectNode> geneStats) {
    val summary = createObject();
    val mergedGeneStats = mergeGeneStats(geneStats);
    for (val featureType : FeatureType.values()) {
      val featureTypeCount = getFeatureTypeCount(featureType, mergedGeneStats);
      val featureTypeSummaryValue = createFeatureTypeSummaryValue(featureType, featureTypeCount);
      summary.put(featureType.getSummaryFieldName(), featureTypeSummaryValue);
    }

    return summary;
  }

  private static int getFeatureTypeCount(FeatureType featureType, ObjectNode geneSummaryTmp) {
    val featureTypeField = geneSummaryTmp.path(featureType.getId());

    return featureTypeField.isMissingNode() ? 0 : featureTypeField.asInt();
  }

  private static ObjectNode mergeGeneStats(Iterable<ObjectNode> geneStats) {
    ObjectNode merged = createObject();
    for (val geneStat : geneStats) {
      removeDonorIdProjectId(geneStat);
      merged = mergeObjects(merged, geneStat);
    }
    removeDonorIdProjectId(merged);

    return merged;
  }

  private static void removeDonorIdProjectId(ObjectNode row) {
    row.remove(DONOR_ID);
    row.remove(PROJECT_ID);
  }

  private static String extractDonorId(Iterable<ObjectNode> geneStats) {
    checkState(!isEmpty(geneStats), "Failed to resolve donor ID from empty collection");
    val geneStat = get(geneStats, 0);

    return textValue(geneStat, DONOR_ID);
  }

  private static void incrementProjectDonors(Map<String, Integer> projectDonors, String projectId) {
    val currentCount = projectDonors.get(projectId);
    projectDonors.put(projectId, currentCount == null ? 1 : currentCount + 1);
  }

  private static String extractProjectId(Iterable<ObjectNode> geneStatsEntry) {
    checkState(!isEmpty(geneStatsEntry),
        "Failed to resolve project name from empty collection. Gene stats entry: %s",
        geneStatsEntry);
    val geneStat = get(geneStatsEntry, 0);

    return textValue(geneStat, PROJECT_ID);
  }

  private static ArrayListMultimap<String, ObjectNode> createDonorIdDonorMultimap(ArrayNode geneStats) {
    val donorIdGeneStats = ArrayListMultimap.<String, ObjectNode> create();
    for (val geneStat : geneStats) {
      val donorId = textValue(geneStat, DONOR_ID);
      donorIdGeneStats.put(donorId, asObjectNode(geneStat));
    }

    return donorIdGeneStats;
  }

}