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
package org.icgc.dcc.release.job.join.function;

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.common.core.model.FeatureTypes.FeatureType.SSM_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.IdentifierFieldNames.SURROGATE_MUTATION_ID;
import static org.icgc.dcc.common.core.model.FieldNames.IdentifierFieldNames.SURROGATE_SAMPLE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.IdentifierFieldNames.SURROGATE_SPECIMEN_ID;
import static org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames.CONSEQUENCE_ARRAY_NAME;
import static org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames.OBSERVATION_ARRAY_NAME;
import static org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames.PROJECT_ID;
import static org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames.SURROGATE_MATCHED_SAMPLE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.NormalizerFieldNames.NORMALIZER_MUTATION;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_ANALYZED_SAMPLE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_MATCHED_SAMPLE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_MUTATION;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_ASSEMBLY_VERSION;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_CHROMOSOME;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_CHROMOSOME_END;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_CHROMOSOME_START;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_CHROMOSOME_STRAND;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_MUTATED_FROM_ALLELE;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_MUTATED_TO_ALLELE;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_MUTATION_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_REFERENCE_GENOME_ALLELE;
import static org.icgc.dcc.release.core.util.ObjectNodes.textValue;
import static org.icgc.dcc.release.job.join.utils.CombineFunctions.mergeConsequences;

import java.util.List;
import java.util.Map;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.icgc.dcc.release.core.function.KeyFields;
import org.icgc.dcc.release.job.join.model.DonorSample;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

@RequiredArgsConstructor
public final class AggregateObservations implements Function2<ObjectNode, ObjectNode, ObjectNode> {

  /**
   * Constants.
   */
  private static final KeyFields PRIMARY_META_KEY_FUNCTION = new KeyAnalysisIdAnalyzedSampleIdField();
  private static final List<String> OCCURRENCE_RETAIN_FIELDS = ImmutableList.of(
      SURROGATE_MUTATION_ID,
      PROJECT_ID,
      SUBMISSION_OBSERVATION_CHROMOSOME,
      SUBMISSION_OBSERVATION_CHROMOSOME_START,
      SUBMISSION_OBSERVATION_CHROMOSOME_END,
      SUBMISSION_OBSERVATION_CHROMOSOME_STRAND,
      CONSEQUENCE_ARRAY_NAME,
      SUBMISSION_OBSERVATION_MUTATED_FROM_ALLELE,
      SUBMISSION_OBSERVATION_MUTATED_TO_ALLELE,
      SUBMISSION_MUTATION,
      SUBMISSION_OBSERVATION_MUTATION_TYPE,
      OBSERVATION_ARRAY_NAME,
      SUBMISSION_OBSERVATION_REFERENCE_GENOME_ALLELE);

  private static final List<String> OBSERVATION_REMOVE_FIELDS = ImmutableList.of(
      SUBMISSION_OBSERVATION_ASSEMBLY_VERSION,
      SUBMISSION_OBSERVATION_MUTATION_TYPE,
      SUBMISSION_OBSERVATION_CHROMOSOME,
      SUBMISSION_OBSERVATION_CHROMOSOME_START,
      SUBMISSION_OBSERVATION_CHROMOSOME_END,
      SUBMISSION_OBSERVATION_CHROMOSOME_STRAND,
      SUBMISSION_OBSERVATION_REFERENCE_GENOME_ALLELE,
      SUBMISSION_OBSERVATION_MUTATED_FROM_ALLELE,
      SUBMISSION_OBSERVATION_MUTATED_TO_ALLELE,
      PROJECT_ID,
      NORMALIZER_MUTATION,
      SURROGATE_MUTATION_ID,
      CONSEQUENCE_ARRAY_NAME);

  /**
   * Dependencies.
   */
  @NonNull
  private final Broadcast<Map<String, ObjectNode>> metaPairsBroadcast;
  @NonNull
  private final Map<String, DonorSample> donorSamples;
  @NonNull
  private final Map<String, String> sampleSurrogageSampleIds;

  @Override
  public ObjectNode call(ObjectNode aggregator, ObjectNode primary) throws Exception {
    val meta = getMeta(getPrimaryMetaKey(primary));
    checkState(meta != null, "A primary record must have a corresponding meta record. Primary: {}", primary);

    val assemblyVersion = textValue(meta, SUBMISSION_OBSERVATION_ASSEMBLY_VERSION);
    val occurrence = aggregator == null ? createOccurrence(primary.deepCopy(), assemblyVersion) : aggregator;

    if (aggregator != null) {
      mergeConsequences(occurrence, primary);
    }

    val observations = occurrence.withArray(OBSERVATION_ARRAY_NAME);
    observations.add(createObservation(primary, meta));

    return occurrence;
  }

  @SneakyThrows
  private static String getPrimaryMetaKey(ObjectNode row) {
    return PRIMARY_META_KEY_FUNCTION.call(row)._1;
  }

  private ObjectNode getMeta(String key) {
    return metaPairsBroadcast.value().get(key);
  }

  private static ObjectNode createOccurrence(ObjectNode primary, String assemblyVersion) {
    val occurrence = trimOccurrence(primary);

    // Enrich with additional fields
    occurrence.put(SUBMISSION_OBSERVATION_ASSEMBLY_VERSION, assemblyVersion);
    occurrence.put(OBSERVATION_TYPE, SSM_TYPE.getId());

    return occurrence;
  }

  private JsonNode createObservation(ObjectNode primary, ObjectNode meta) {
    val sampleId = textValue(primary, SUBMISSION_ANALYZED_SAMPLE_ID);
    primary.setAll(meta);
    primary.put(SURROGATE_SPECIMEN_ID, donorSamples.get(sampleId).getSpecimenId());
    primary.put(SURROGATE_SAMPLE_ID, donorSamples.get(sampleId).getSampleId());

    val matchedSampleId = textValue(meta, SUBMISSION_MATCHED_SAMPLE_ID);
    primary.put(SURROGATE_MATCHED_SAMPLE_ID, sampleSurrogageSampleIds.get(matchedSampleId));

    return trimObservation(primary);
  }

  /**
   * Removes fields in the parent object.
   */
  private static ObjectNode trimOccurrence(ObjectNode occurrence) {
    return occurrence.retain(OCCURRENCE_RETAIN_FIELDS);
  }

  private static ObjectNode trimObservation(ObjectNode observation) {
    return observation.remove(OBSERVATION_REMOVE_FIELDS);
  }

}