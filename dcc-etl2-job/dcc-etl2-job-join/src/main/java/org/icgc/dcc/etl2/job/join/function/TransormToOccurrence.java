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
package org.icgc.dcc.etl2.job.join.function;

import static org.icgc.dcc.common.core.model.FeatureTypes.FeatureType.SSM_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_VERIFICATION_STATUS;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_VERIFICATION_PLATFORM;
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
import static org.icgc.dcc.common.core.model.FieldNames.NormalizerFieldNames.NORMALIZER_MARKING;
import static org.icgc.dcc.common.core.model.FieldNames.NormalizerFieldNames.NORMALIZER_OBSERVATION_ID;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_ANALYZED_SAMPLE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_GENE_AFFECTED;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_MATCHED_SAMPLE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_MUTATION;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_ANALYSIS_ID;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_ASSEMBLY_VERSION;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_CHROMOSOME;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_CHROMOSOME_END;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_CHROMOSOME_START;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_CHROMOSOME_STRAND;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_CONTROL_GENOTYPE;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_MUTATED_FROM_ALLELE;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_MUTATED_TO_ALLELE;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_MUTATION_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_REFERENCE_GENOME_ALLELE;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_TUMOUR_GENOTYPE;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_TRANSCRIPT_AFFECTED;
import static org.icgc.dcc.etl2.core.util.Keys.KEY_SEPARATOR;
import static org.icgc.dcc.etl2.core.util.ObjectNodes.textValue;

import java.util.Map;

import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.spark.api.java.function.Function;
import org.icgc.dcc.etl2.job.join.model.Donor;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

@RequiredArgsConstructor
public class TransormToOccurrence implements Function<Tuple2<String, Iterable<Tuple2<String, Tuple2<Tuple2<ObjectNode,
    Iterable<ObjectNode>>, ObjectNode>>>>, ObjectNode> {

  private static final int DONOR_ID_INDEX = 0;
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final Map<String, Donor> sampleDonorIds;
  private final Map<String, String> sampleSurrogageSampleIds;

  @Override
  public ObjectNode call(Tuple2<String, Iterable<Tuple2<String, Tuple2<Tuple2<ObjectNode, Iterable<ObjectNode>>,
      ObjectNode>>>> tuple) throws Exception {
    ObjectNode mutation = null;
    ArrayNode consequences = null;
    val observations = createObservations();

    val ssms = tuple._2;
    for (val ssm : ssms) {
      val primary = ssm._2._1._1;
      val meta = ssm._2._2;

      if (mutation == null) {
        val donorIdMutationId = tuple._1;
        mutation = createMutation(primary, meta, donorIdMutationId);
      }

      val observation = createObservation(primary.deepCopy(), meta.deepCopy());
      observations.add(observation);

      if (consequences == null) {
        val secondaries = ssm._2._1._2;
        consequences = createConsequences(secondaries);
      }
    }

    mutation.put(CONSEQUENCE_ARRAY_NAME, consequences);
    mutation.put(OBSERVATION_ARRAY_NAME, observations);

    return mutation;
  }

  private static ObjectNode createMutation(ObjectNode primary, ObjectNode meta, String donorIdMutationId) {
    val mutation = trimMutation(primary.deepCopy());

    // Enrich with additional fields
    mutation.put(SURROGATE_DONOR_ID, resolveDonorId(donorIdMutationId));
    mutation.put(SUBMISSION_OBSERVATION_ASSEMBLY_VERSION, textValue(meta, SUBMISSION_OBSERVATION_ASSEMBLY_VERSION));

    // TODO: resolve dynamically?
    mutation.put(OBSERVATION_TYPE, SSM_TYPE.getId());

    return mutation;
  }

  private JsonNode createObservation(ObjectNode primary, ObjectNode meta) {
    val sampleId = textValue(primary, SUBMISSION_ANALYZED_SAMPLE_ID);
    primary.putAll(meta);
    primary.put(SURROGATE_SPECIMEN_ID, sampleDonorIds.get(sampleId).getSpecimenId());
    primary.put(SURROGATE_SAMPLE_ID, sampleDonorIds.get(sampleId).getSampleId());

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
  // TODO: add fields to FieldNames
  private static ObjectNode trimMutation(ObjectNode mutation) {
    mutation.remove(SUBMISSION_OBSERVATION_ANALYSIS_ID);
    mutation.remove(SUBMISSION_ANALYZED_SAMPLE_ID);
    mutation.remove("expressed_allele");
    mutation.remove(SUBMISSION_OBSERVATION_TUMOUR_GENOTYPE);
    mutation.remove("quality_score");
    mutation.remove("probability");
    mutation.remove("total_read_count");
    mutation.remove("mutant_allele_read_count");
    mutation.remove(MUTATION_VERIFICATION_STATUS);
    mutation.remove(OBSERVATION_VERIFICATION_PLATFORM);
    mutation.remove("biological_validation_status");
    mutation.remove("biological_validation_platform");
    mutation.remove(NORMALIZER_OBSERVATION_ID);
    mutation.remove(NORMALIZER_MARKING);
    mutation.remove(SUBMISSION_OBSERVATION_CONTROL_GENOTYPE);

    return mutation;
  }

  private static ObjectNode trimObservation(ObjectNode observation) {
    observation.remove(SUBMISSION_OBSERVATION_ASSEMBLY_VERSION);
    observation.remove(SUBMISSION_OBSERVATION_MUTATION_TYPE);
    observation.remove(SUBMISSION_OBSERVATION_CHROMOSOME);
    observation.remove(SUBMISSION_OBSERVATION_CHROMOSOME_START);
    observation.remove(SUBMISSION_OBSERVATION_CHROMOSOME_END);
    observation.remove(SUBMISSION_OBSERVATION_CHROMOSOME_STRAND);
    observation.remove(SUBMISSION_OBSERVATION_REFERENCE_GENOME_ALLELE);
    observation.remove(SUBMISSION_OBSERVATION_CONTROL_GENOTYPE);
    observation.remove(SUBMISSION_OBSERVATION_MUTATED_FROM_ALLELE);
    observation.remove(SUBMISSION_OBSERVATION_MUTATED_TO_ALLELE);
    observation.remove(SUBMISSION_OBSERVATION_TUMOUR_GENOTYPE);
    observation.remove("expressed_allele");
    observation.remove("quality_score");
    observation.remove("probability");
    observation.remove(OBSERVATION_VERIFICATION_PLATFORM);
    observation.remove("biological_validation_platform");
    observation.remove(PROJECT_ID);
    observation.remove(SUBMISSION_MUTATION);
    observation.remove(SURROGATE_MUTATION_ID);
    observation.remove("experimental_protocol");

    return observation;
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
    // FIXME: clear null values fields (E.g. cds_mutation: null is not in the Mongo db)?
    consequence.remove(NORMALIZER_OBSERVATION_ID);
    consequence.put(GENE_ID, consequence.get(SUBMISSION_GENE_AFFECTED));
    consequence.put(TRANSCRIPT_ID, consequence.get(SUBMISSION_TRANSCRIPT_AFFECTED));
  }

  private static ArrayNode createObservations() {
    return MAPPER.createArrayNode();
  }

}