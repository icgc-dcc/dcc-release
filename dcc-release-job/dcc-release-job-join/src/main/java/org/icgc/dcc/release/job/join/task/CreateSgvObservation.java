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

import static org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames.CONSEQUENCE_ARRAY_NAME;
import static org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames.SURROGATE_MATCHED_SAMPLE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.NormalizerFieldNames.NORMALIZER_OBSERVATION_ID;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_MATCHED_SAMPLE_ID;
import static org.icgc.dcc.release.core.util.ObjectNodes.textValue;
import static org.icgc.dcc.release.job.join.function.AggregateConsequences.REMOVE_CONSEQUENCE_FIELDS;
import static org.icgc.dcc.release.job.join.utils.Consequences.enrichConsequence;

import java.util.Collection;
import java.util.Map;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.icgc.dcc.release.core.util.JacksonFactory;
import org.icgc.dcc.release.core.util.ObjectNodes;
import org.icgc.dcc.release.job.join.model.SgvConsequence;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

@RequiredArgsConstructor
public final class CreateSgvObservation implements Function<ObjectNode, ObjectNode> {

  @NonNull
  private final String projectName;
  @NonNull
  private final Broadcast<Map<String, Iterable<SgvConsequence>>> consequencesBroadcast;
  @NonNull
  private final Broadcast<Map<String, Map<String, String>>> sampleSurrogateSampleIdsByProject;

  @Override
  public ObjectNode call(ObjectNode row) throws Exception {
    val observationId = ObjectNodes.textValue(row, NORMALIZER_OBSERVATION_ID);
    val consequences = convertConsequences(getConsequences(observationId));
    val consequenceArray = row.withArray(CONSEQUENCE_ARRAY_NAME);
    if (consequences.isPresent()) {
      consequenceArray.addAll(consequences.get());
    }

    // add surrogate key
    val matchedSampleId = textValue(row, SUBMISSION_MATCHED_SAMPLE_ID);
    if (matchedSampleId != null) {
      row.put(SURROGATE_MATCHED_SAMPLE_ID, resolveSampleSurrogateId(matchedSampleId));
    }

    return row;
  }

  private String resolveSampleSurrogateId(String matchedSampleId) {
    return sampleSurrogateSampleIdsByProject.value().get(projectName).get(matchedSampleId);
  }

  public static Optional<Collection<ObjectNode>> convertConsequences(
      Optional<? extends Iterable<SgvConsequence>> consequences) {
    if (!consequences.isPresent()) {
      return Optional.absent();
    }

    val builder = ImmutableList.<ObjectNode> builder();
    for (val consequence : consequences.get()) {
      val json = (ObjectNode) JacksonFactory.MAPPER.valueToTree(consequence);
      builder.add(trimConsequence(json));
    }

    return Optional.of(builder.build());
  }

  private static ObjectNode trimConsequence(ObjectNode consequence) {
    consequence.remove(REMOVE_CONSEQUENCE_FIELDS);
    enrichConsequence(consequence);

    return consequence;
  }

  /**
   * @param observationId
   * @return
   */
  private Optional<Iterable<SgvConsequence>> getConsequences(String observationId) {
    val consequences = consequencesBroadcast.value().get(observationId);

    return Optional.fromNullable(consequences);
  }
}