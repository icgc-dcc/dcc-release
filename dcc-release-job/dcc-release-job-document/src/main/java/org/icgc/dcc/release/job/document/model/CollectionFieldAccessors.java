/*
 * Copyright (c) 2014 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.release.job.document.model;

import static com.google.common.base.Strings.emptyToNull;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.model.FeatureTypes.FeatureType.SSM_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_GENES;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_GENE_GENE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_ID;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_PROJECT;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_PROJECT_ID;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_DONORS;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_DONOR_DONOR_ID;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_PROJECTS;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_PROJECT_PROJECT_ID;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_SETS;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_SETS_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_SET_ID;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_TRANSCRIPTS;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_TRANSCRIPTS_TRANSCRIPT_ID;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_ID;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_OBSERVATIONS;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_OBSERVATION_DONOR;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_OBSERVATION_PROJECT;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_OCCURRENCES;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_TRANSCRIPTS;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_TRANSCRIPTS_CONSEQUENCE;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_TRANSCRIPTS_FUNCTIONAL_IMPACT_PREDICTION;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_TRANSCRIPTS_FUNCTIONAL_IMPACT_PREDICTION_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES_CONSEQUENCE_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES_GENE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES_TRANSCRIPT_ID;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_DONOR;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_DONOR_ID;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_ID;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_IS_ANNOTATED;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_MUTATION_ID;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_PLATFORM;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_PROJECT;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_SEQUENCING_STRATEGY;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_VALIDATION_STATUS;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_VERIFICATION_STATUS;
import static org.icgc.dcc.common.core.model.FieldNames.PROJECT_ID;
import static org.icgc.dcc.common.core.model.FieldNames.PROJECT_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.getTestedTypeCountFieldName;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.val;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;

/**
 * Static accessor methods for dynamic collection objects.
 */
@NoArgsConstructor(access = PRIVATE)
public final class CollectionFieldAccessors {

  public static String getProjectId(@NonNull JsonNode project) {
    return project.get(PROJECT_ID).asText();
  }

  public static int getProjectTestedDonorSsmCount(@NonNull JsonNode project) {
    return project.path(PROJECT_SUMMARY).path(getTestedTypeCountFieldName(SSM_TYPE)).asInt();
  }

  public static String getDonorId(@NonNull JsonNode donor) {
    return donor.get(DONOR_ID).asText();
  }

  public static ArrayNode getDonorGenes(@NonNull ObjectNode donor) {
    return donor.withArray(DONOR_GENES);
  }

  public static String getDonorGeneId(@NonNull JsonNode donorGene) {
    return donorGene.get(DONOR_GENE_GENE_ID).textValue();
  }

  public static String getDonorProjectId(@NonNull JsonNode donor) {
    return donor.get(DONOR_PROJECT_ID).asText();
  }

  public static void setDonorProject(@NonNull ObjectNode donor, @NonNull JsonNode donorProject) {
    donor.set(DONOR_PROJECT, donorProject);
  }

  public static String getGeneId(@NonNull JsonNode gene) {
    return gene.get(GENE_ID).asText();
  }

  public static String getGeneDonorId(@NonNull JsonNode gene) {
    return gene.get(GENE_DONOR_DONOR_ID).asText();
  }

  public static ArrayNode getGeneDonors(@NonNull ObjectNode gene) {
    return gene.withArray(GENE_DONORS);
  }

  public static String getGeneProjectId(@NonNull JsonNode gene) {
    return gene.get(GENE_PROJECT_PROJECT_ID).asText();
  }

  public static ArrayNode getGeneProjects(@NonNull ObjectNode gene) {
    return gene.withArray(GENE_PROJECTS);
  }

  public static JsonNode getGeneGeneSets(@NonNull JsonNode gene) {
    return gene.path(GENE_SETS);
  }

  public static String getGeneGeneSetId(@NonNull JsonNode geneGeneSet) {
    return geneGeneSet.get(GENE_SET_ID).asText();
  }

  public static String getGeneGeneSetType(@NonNull JsonNode geneGeneSet) {
    return geneGeneSet.get(GENE_SETS_TYPE).asText();
  }

  public static void removeGeneGeneSets(@NonNull ObjectNode gene) {
    gene.remove(GENE_SETS);
  }

  public static ArrayNode getGeneTranscripts(@NonNull ObjectNode gene) {
    return gene.withArray(GENE_TRANSCRIPTS);
  }

  public static String getGeneSetId(@NonNull JsonNode geneSet) {
    return geneSet.get(GENE_SET_ID).asText();
  }

  public static String getTranscriptId(@NonNull JsonNode transcript) {
    return getText(transcript, GENE_TRANSCRIPTS_TRANSCRIPT_ID);
  }

  public static String getConsequenceType(@NonNull JsonNode consequence) {
    return getText(consequence, OBSERVATION_CONSEQUENCES_CONSEQUENCE_TYPE);
  }

  public static String getObservationId(@NonNull JsonNode observation) {
    return observation.get(OBSERVATION_ID).textValue();
  }

  public static String getObservationMutationId(@NonNull JsonNode observation) {
    return observation.get(OBSERVATION_MUTATION_ID).textValue();
  }

  public static String getObservationDonorId(@NonNull JsonNode observation) {
    return observation.get(OBSERVATION_DONOR_ID).textValue();
  }

  public static String getObservationType(@NonNull JsonNode observation) {
    return observation.get(OBSERVATION_TYPE).textValue();
  }

  public static String getObservationPlatform(@NonNull JsonNode mutation) {
    return getText(mutation, OBSERVATION_PLATFORM);
  }

  public static String getObservationValidationStatus(@NonNull JsonNode mutation) {
    return getText(mutation, OBSERVATION_VALIDATION_STATUS);
  }

  public static String getObservationVerificationStatus(@NonNull JsonNode mutation) {
    return getText(mutation, OBSERVATION_VERIFICATION_STATUS);
  }

  public static String getObservationIsAnnotated(JsonNode mutation) {
    return getText(mutation, OBSERVATION_IS_ANNOTATED);
  }

  public static JsonNode getObservationConsequences(@NonNull JsonNode observation) {
    return observation.path(OBSERVATION_CONSEQUENCES);
  }

  public static String getObservationConsequenceGeneId(@NonNull JsonNode consequence) {
    return getText(consequence, OBSERVATION_CONSEQUENCES_GENE_ID);
  }

  public static Iterable<String> getObservationConsequenceGeneIds(@NonNull ObjectNode observation) {
    val geneIds = Lists.<String> newArrayList();
    val consequences = getObservationConsequences(observation);
    for (val consequence : consequences) {
      val geneId = getObservationConsequenceGeneId(consequence);

      geneIds.add(geneId);
    }

    return geneIds;
  }

  public static String getObservationSequenceStrategy(@NonNull JsonNode observation) {
    return getText(observation, OBSERVATION_SEQUENCING_STRATEGY);
  }

  public static String getObservationConsequenceTranscriptId(@NonNull JsonNode consequence) {
    return getText(consequence, OBSERVATION_CONSEQUENCES_TRANSCRIPT_ID);
  }

  public static void removeObservationConsequenceGeneId(@NonNull JsonNode consequence) {
    ((ObjectNode) consequence).remove(OBSERVATION_CONSEQUENCES_GENE_ID);
  }

  public static void removeObservationConsequences(@NonNull JsonNode observation) {
    ((ObjectNode) observation).remove(OBSERVATION_CONSEQUENCES);
  }

  public static void setObservationProject(@NonNull ObjectNode observation, @NonNull ObjectNode project) {
    observation.set(OBSERVATION_PROJECT, project);
  }

  public static void setObservationDonor(@NonNull ObjectNode observation, @NonNull ObjectNode donor) {
    observation.set(OBSERVATION_DONOR, donor);
  }

  public static String getMutationId(@NonNull ObjectNode mutation) {
    return mutation.get(MUTATION_ID).textValue();
  }

  public static ArrayNode getOccurrenceObservations(@NonNull ObjectNode occurrence) {
    return occurrence.withArray(MUTATION_OBSERVATIONS);
  }

  public static ArrayNode getMutationOccurrences(@NonNull ObjectNode mutation) {
    return mutation.withArray(MUTATION_OCCURRENCES);
  }

  public static JsonNode getMutationObservationDonor(@NonNull ObjectNode mutation) {
    return mutation.get(MUTATION_OBSERVATION_DONOR);
  }

  public static JsonNode getMutationObservationProject(@NonNull ObjectNode mutation) {
    return mutation.get(MUTATION_OBSERVATION_PROJECT);
  }

  public static ArrayNode getMutationTranscripts(ObjectNode mutation) {
    return mutation.withArray(MUTATION_TRANSCRIPTS);
  }

  public static JsonNode getMutationTranscriptConsequence(ObjectNode mutationTranscript) {
    return mutationTranscript.get(MUTATION_TRANSCRIPTS_CONSEQUENCE);
  }

  public static String getMutationTranscriptImpactPredictionSummary(@NonNull JsonNode mutationTranscript) {
    return getText(mutationTranscript, MUTATION_TRANSCRIPTS_FUNCTIONAL_IMPACT_PREDICTION_SUMMARY);
  }

  public static String getMutationTranscriptImpactPrediction(@NonNull JsonNode mutationTranscript) {
    return getText(mutationTranscript, MUTATION_TRANSCRIPTS_FUNCTIONAL_IMPACT_PREDICTION);
  }

  public static String getGeneSetId(@NonNull ObjectNode geneSet) {
    return geneSet.get(GENE_SET_ID).asText();
  }

  /**
   * Utilities
   */

  public static String getText(@NonNull JsonNode node, String property) {
    return emptyToNull(node.path(property).textValue());
  }

  public static boolean isBlank(@NonNull JsonNode node, String property) {
    return getText(node, property) == null;
  }

}
