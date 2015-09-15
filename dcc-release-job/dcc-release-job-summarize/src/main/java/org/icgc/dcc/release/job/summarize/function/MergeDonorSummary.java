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
package org.icgc.dcc.release.job.summarize.function;

import static java.util.Collections.emptyMap;
import static org.icgc.dcc.common.core.model.FieldNames.AVAILABLE_DATA_TYPES;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SUMMARY_STATE;
import static org.icgc.dcc.release.core.util.FeatureTypes.createFeatureTypeSummaryValue;
import static org.icgc.dcc.release.core.util.FeatureTypes.getFeatureTypes;
import static org.icgc.dcc.release.core.util.ObjectNodes.MAPPER;
import static org.icgc.dcc.release.core.util.ObjectNodes.isEmpty;
import static org.icgc.dcc.release.core.util.ObjectNodes.mergeObjects;
import static org.icgc.dcc.release.core.util.Tuples.tuple;

import java.util.Map;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;

@RequiredArgsConstructor
public final class MergeDonorSummary implements
    PairFunction<Tuple2<String, Tuple2<ObjectNode, Optional<ObjectNode>>>, String, ObjectNode> {

  @NonNull
  private final Broadcast<Map<String, Map<String, ObjectNode>>> projectDonorSummary;
  @NonNull
  private final String projectName;

  @Override
  public Tuple2<String, ObjectNode> call(Tuple2<String, Tuple2<ObjectNode, Optional<ObjectNode>>> tuple)
      throws Exception {
    val donorId = tuple._1;
    val donorSummary = tuple._2._1;
    val geneDonorSummaryOpt = tuple._2._2;

    ObjectNode mergedSummary = donorSummary;
    if (geneDonorSummaryOpt.isPresent()) {
      mergedSummary = mergeObjects(geneDonorSummaryOpt.get(), donorSummary);
    }

    val featureTypeSummaries = resolveFeatureTypeSummaries();
    val featureTypeSummary = resolveFeatureTypeSummary(featureTypeSummaries.get(donorId));

    return tuple(donorId, mergeObjects(mergedSummary, featureTypeSummary));
  }

  private ObjectNode resolveFeatureTypeSummary(ObjectNode featureTypeSummary) {
    val resultSummaryNode = featureTypeSummary == null ? MAPPER.createObjectNode() : featureTypeSummary;
    val summary = resultSummaryNode.with(DONOR_SUMMARY);
    val availableDataTypes = summary.withArray(AVAILABLE_DATA_TYPES);
    val state = isEmpty(availableDataTypes) ? "pending" : "live";
    summary.put(DONOR_SUMMARY_STATE, state);

    for (val featureType : getFeatureTypes()) {
      val featureTypeSummaryFieldName = featureType.getSummaryFieldName();
      val featureTypeSummaryNode = summary.path(featureTypeSummaryFieldName);
      if (featureTypeSummaryNode.isMissingNode()) {
        val featureTypeSummaryFieldValue = createFeatureTypeSummaryValue(featureType, 0);
        summary.put(featureTypeSummaryFieldName, featureTypeSummaryFieldValue);
      }
    }

    return resultSummaryNode;
  }

  private Map<String, ObjectNode> resolveFeatureTypeSummaries() {
    val donorSummary = projectDonorSummary.getValue().get(projectName);

    return donorSummary == null ? emptyMap() : donorSummary;
  }

}