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
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.NormalizerFieldNames.NORMALIZER_OBSERVATION_ID;
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

    mutation.put("consequence", consequences);
    mutation.put("observation", observations);

    return mutation;
  }

  private static ObjectNode createMutation(ObjectNode primary, ObjectNode meta, String donorIdMutationId) {
    val mutation = trimMutation(primary.deepCopy());

    // Enrich with additional fields
    mutation.put("_donor_id", resolveDonorId(donorIdMutationId));
    mutation.put("assembly_version", textValue(meta, "assembly_version"));

    // TODO: resolve dynamically?
    mutation.put(OBSERVATION_TYPE, SSM_TYPE.getId());

    return mutation;
  }

  private JsonNode createObservation(ObjectNode primary, ObjectNode meta) {
    val sampleId = textValue(primary, "analyzed_sample_id");
    primary.putAll(meta);
    primary.put("_specimen_id", sampleDonorIds.get(sampleId).getSpecimenId());
    primary.put("_sample_id", sampleDonorIds.get(sampleId).getSampleId());

    val matchedSampleId = textValue(meta, "matched_sample_id");
    primary.put("_matched_sample_id", sampleSurrogageSampleIds.get(matchedSampleId));
    // add _matched_sample_id

    return trimObservation(primary);
  }

  private static String resolveDonorId(String donorIdMutationId) {
    return donorIdMutationId.split(KEY_SEPARATOR)[DONOR_ID_INDEX];
  }

  /**
   * Removes fields in the parent object.
   */
  // TODO: use FieldNames
  private static ObjectNode trimMutation(ObjectNode mutation) {
    mutation.remove("analysis_id");
    mutation.remove("analyzed_sample_id");
    mutation.remove("expressed_allele");
    mutation.remove("tumour_genotype");
    mutation.remove("quality_score");
    mutation.remove("probability");
    mutation.remove("total_read_count");
    mutation.remove("mutant_allele_read_count");
    mutation.remove("verification_status");
    mutation.remove("verification_platform");
    mutation.remove("biological_validation_status");
    mutation.remove("biological_validation_platform");
    mutation.remove("observation_id");
    mutation.remove("marking");
    mutation.remove("control_genotype");

    return mutation;
  }

  // TODO: use FieldNames
  private static ObjectNode trimObservation(ObjectNode observation) {
    observation.remove("assembly_version");
    observation.remove("mutation_type");
    observation.remove("chromosome");
    observation.remove("chromosome_start");
    observation.remove("chromosome_end");
    observation.remove("chromosome_strand");
    observation.remove("reference_genome_allele");
    observation.remove("control_genotype");
    observation.remove("mutated_from_allele");
    observation.remove("mutated_to_allele");
    observation.remove("tumour_genotype");
    observation.remove("expressed_allele");
    observation.remove("quality_score");
    observation.remove("probability");
    observation.remove("verification_platform");
    observation.remove("biological_validation_platform");
    observation.remove("_project_id");
    observation.remove("mutation");
    observation.remove("_mutation_id");
    observation.remove("experimental_protocol");

    return observation;
  }

  private static ArrayNode createConsequences(Iterable<ObjectNode> secondaries) {
    val consequences = MAPPER.createArrayNode();
    for (val secondary : secondaries) {
      trimConsequence(secondary);
      consequences.add(secondary);
    }

    return consequences;
  }

  private static void trimConsequence(ObjectNode consequence) {
    // FIXME: add _gene_id and clear null values fields (E.g. cds_mutation: null is not in the Mongo db)?
    // FIXME: add _transcript_id ?

    consequence.remove(NORMALIZER_OBSERVATION_ID);
  }

  private static ArrayNode createObservations() {
    return MAPPER.createArrayNode();
  }

}