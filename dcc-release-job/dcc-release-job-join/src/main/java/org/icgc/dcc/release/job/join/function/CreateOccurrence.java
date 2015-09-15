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

import static org.icgc.dcc.common.core.model.FeatureTypes.FeatureType.SSM_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.IdentifierFieldNames.SURROGATE_DONOR_ID;
import static org.icgc.dcc.common.core.model.FieldNames.IdentifierFieldNames.SURROGATE_MUTATION_ID;
import static org.icgc.dcc.common.core.model.FieldNames.IdentifierFieldNames.SURROGATE_SAMPLE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.IdentifierFieldNames.SURROGATE_SPECIMEN_ID;
import static org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames.CONSEQUENCE_ARRAY_NAME;
import static org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames.GENE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames.OBSERVATION_ARRAY_NAME;
import static org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames.PROJECT_ID;
import static org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames.SURROGATE_MATCHED_SAMPLE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames.TRANSCRIPT_ID;
import static org.icgc.dcc.common.core.model.FieldNames.NormalizerFieldNames.NORMALIZER_MUTATION;
import static org.icgc.dcc.common.core.model.FieldNames.NormalizerFieldNames.NORMALIZER_OBSERVATION_ID;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_ANALYZED_SAMPLE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_GENE_AFFECTED;
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
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_TRANSCRIPT_AFFECTED;
import static org.icgc.dcc.common.core.util.Jackson.DEFAULT;
import static org.icgc.dcc.release.core.util.Keys.KEY_SEPARATOR;
import static org.icgc.dcc.release.core.util.ObjectNodes.textValue;

import java.util.List;
import java.util.Map;

import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.spark.api.java.function.Function;
import org.icgc.dcc.release.job.join.model.DonorSample;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

@RequiredArgsConstructor
public class CreateOccurrence implements Function<Tuple2<String, Iterable<Tuple2<String, Tuple2<Tuple2<ObjectNode,
    Iterable<ObjectNode>>, ObjectNode>>>>, ObjectNode> {

  private static final int DONOR_ID_INDEX = 0;
  private static final ObjectMapper MAPPER = DEFAULT;
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
      SURROGATE_MUTATION_ID);

  private final Map<String, DonorSample> donorSamples;
  private final Map<String, String> sampleSurrogageSampleIds;

  @Override
  public ObjectNode call(Tuple2<String, Iterable<Tuple2<String, Tuple2<Tuple2<ObjectNode, Iterable<ObjectNode>>,
      ObjectNode>>>> tuple) throws Exception {
    ObjectNode occurrence = null;
    ArrayNode consequences = null;
    val observations = createObservations();

    val ssms = tuple._2;
    for (val ssm : ssms) {
      val primary = ssm._2._1._1;
      val meta = ssm._2._2;

      if (occurrence == null) {
        val donorIdMutationId = tuple._1;
        occurrence = createOccurrence(primary, meta, donorIdMutationId);
      }

      val observation = createObservation(primary.deepCopy(), meta.deepCopy());
      observations.add(observation);

      if (consequences == null) {
        val secondaries = ssm._2._1._2;
        consequences = createConsequences(secondaries);
      }
    }

    occurrence.put(CONSEQUENCE_ARRAY_NAME, consequences);
    occurrence.put(OBSERVATION_ARRAY_NAME, observations);

    return occurrence;
  }

  private static ObjectNode createOccurrence(ObjectNode primary, ObjectNode meta, String donorIdMutationId) {
    val occurrence = trimOccurrence(primary.deepCopy());

    // Enrich with additional fields
    occurrence.put(SURROGATE_DONOR_ID, resolveDonorId(donorIdMutationId));
    occurrence.put(SUBMISSION_OBSERVATION_ASSEMBLY_VERSION, textValue(meta, SUBMISSION_OBSERVATION_ASSEMBLY_VERSION));
    occurrence.put(OBSERVATION_TYPE, SSM_TYPE.getId());

    return occurrence;
  }

  private JsonNode createObservation(ObjectNode primary, ObjectNode meta) {
    val sampleId = textValue(primary, SUBMISSION_ANALYZED_SAMPLE_ID);
    primary.putAll(meta);
    primary.put(SURROGATE_SPECIMEN_ID, donorSamples.get(sampleId).getSpecimenId());
    primary.put(SURROGATE_SAMPLE_ID, donorSamples.get(sampleId).getSampleId());

    val matchedSampleId = textValue(meta, SUBMISSION_MATCHED_SAMPLE_ID);
    primary.put(SURROGATE_MATCHED_SAMPLE_ID, sampleSurrogageSampleIds.get(matchedSampleId));

    return trimObservation(primary);
  }

  private static String resolveDonorId(String donorIdMutationId) {
    return donorIdMutationId.split(KEY_SEPARATOR)[DONOR_ID_INDEX];
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

  private static ArrayNode createConsequences(Iterable<ObjectNode> secondaries) {
    val consequences = MAPPER.createArrayNode();
    for (val secondary : secondaries) {
      enrichConsequenceFields(secondary);
      consequences.add(secondary);
    }

    return consequences;
  }

  private static void enrichConsequenceFields(ObjectNode consequence) {
    consequence.remove(NORMALIZER_OBSERVATION_ID);
    consequence.put(GENE_ID, consequence.get(SUBMISSION_GENE_AFFECTED));
    consequence.put(TRANSCRIPT_ID, consequence.get(SUBMISSION_TRANSCRIPT_AFFECTED));
  }

  private static ArrayNode createObservations() {
    return MAPPER.createArrayNode();
  }

}