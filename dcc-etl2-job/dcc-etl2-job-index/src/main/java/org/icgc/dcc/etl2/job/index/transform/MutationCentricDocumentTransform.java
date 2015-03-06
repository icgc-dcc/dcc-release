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
package org.icgc.dcc.etl2.job.index.transform;

import static com.google.common.base.Objects.firstNonNull;
import static com.google.common.collect.Lists.newArrayList;
import static org.icgc.dcc.common.core.model.FieldNames.CONSEQUENCE_AA_MUTATION;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_SYMBOL;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_TRANSCRIPTS;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_OBSERVATION_DONOR;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_OBSERVATION_PROJECT;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_TRANSCRIPTS_CONSEQUENCE;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_TRANSCRIPTS_FUNCTIONAL_IMPACT_PREDICTION;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_TRANSCRIPTS_FUNCTIONAL_IMPACT_PREDICTION_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_TRANSCRIPTS_GENE;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES_CONSEQUENCE_FUNCTIONAL_IMPACT_PREDICTION;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES_CONSEQUENCE_FUNCTIONAL_IMPACT_PREDICTION_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCE_TYPES;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_FUNCTIONAL_IMPACT_PREDICTION_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_ID;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_IS_ANNOTATED;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_MUTATION_ID;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_TYPE;
import static org.icgc.dcc.etl2.job.index.model.CollectionFieldAccessors.getDonorProjectId;
import static org.icgc.dcc.etl2.job.index.model.CollectionFieldAccessors.getGeneTranscripts;
import static org.icgc.dcc.etl2.job.index.model.CollectionFieldAccessors.getMutationId;
import static org.icgc.dcc.etl2.job.index.model.CollectionFieldAccessors.getMutationOccurrences;
import static org.icgc.dcc.etl2.job.index.model.CollectionFieldAccessors.getMutationTranscripts;
import static org.icgc.dcc.etl2.job.index.model.CollectionFieldAccessors.getObservationConsequenceGeneId;
import static org.icgc.dcc.etl2.job.index.model.CollectionFieldAccessors.getObservationConsequenceTranscriptId;
import static org.icgc.dcc.etl2.job.index.model.CollectionFieldAccessors.getObservationConsequences;
import static org.icgc.dcc.etl2.job.index.model.CollectionFieldAccessors.getObservationDonorId;
import static org.icgc.dcc.etl2.job.index.model.CollectionFieldAccessors.getTranscriptId;
import static org.icgc.dcc.etl2.job.index.model.CollectionFieldAccessors.setObservationDonor;
import static org.icgc.dcc.etl2.job.index.model.CollectionFieldAccessors.setObservationProject;
import static org.icgc.dcc.etl2.job.index.util.Fakes.FAKE_GENE_ID;
import static org.icgc.dcc.etl2.job.index.util.Fakes.FAKE_TRANSCRIPT_ID;
import static org.icgc.dcc.etl2.job.index.util.Fakes.createFakeGene;
import static org.icgc.dcc.etl2.job.index.util.Fakes.isFakeGeneId;
import static org.icgc.dcc.etl2.job.index.util.JsonNodes.defaultMissing;
import static org.icgc.dcc.etl2.job.index.util.JsonNodes.defaultObject;

import java.util.List;
import java.util.TreeMap;

import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.common.core.model.BusinessKeys;
import org.icgc.dcc.etl2.job.index.core.Document;
import org.icgc.dcc.etl2.job.index.core.DocumentCallback;
import org.icgc.dcc.etl2.job.index.core.DocumentContext;
import org.icgc.dcc.etl2.job.index.core.DocumentTransform;
import org.icgc.dcc.etl2.job.index.util.Fakes;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Maps;

/**
 * {@link DocumentTransform} implementation that creates a nested mutation-centric document:
 * 
 * <pre>
 * {
 *  _mutation_id: 'MU1',
 *  ...
 *  consequence_type: ["ct1"],
 *  is_annotated: [ "p1" ],
 *  platform: [ "p1" ],
 *  verification_status: [ "vfs1" ],
 *  validation_stats: [ "vls1" ],
 *  _summary: {
 *    ...
 *  },
 *  ssm_occurrence: [ {
 *    donor: {
 *      _donor_id: 'DO1',
 *      _project_id: 'PR1',
 *      ... 
 *    },
 *    project: {
 *      _project_id: 'PR1',
 *      ... 
 *    }
 *    ...
 *  } ],
 *  transcript: [ {
 *    _transcript_id: 'tr1',
 *    ...
 *    gene: {
 *      _gene_id: 'g1',
 *      ...
 *    },
 *    consequence: {
 *      consequence_type: 'ct1',
 *      ...
 *    }
 *  } ]
 * }
 * </pre>
 */
public class MutationCentricDocumentTransform extends AbstractCentricDocumentTransform {

  private static DocumentCallback SUMMARY_CALLBACK = new MutationCentricSummaryCallback();

  @Override
  public Document transformDocument(@NonNull ObjectNode mutation, @NonNull DocumentContext context) {

    // Indexes
    // - f(transcriptId) -> transcript
    val mutationTranscriptMap = newTreeMap();
    // - f(transcriptId) -> gene
    val mutationTranscriptGeneMap = newTreeMap();
    // - f(transcriptId) -> consequence
    val mutationTranscriptConsequenceMap = newTreeMap();

    /**
     * Mutation: {@code mutation}.
     */

    // - {@code mutation.ssm_occurrence}
    val mutationSsmOccurrences = getMutationOccurrences(mutation);

    // Get observations of this mutation
    val mutationId = getMutationId(mutation);
    val mutationObservations = context.getObservationsByMutationId(mutationId);
    val mutationTranscripts = getMutationTranscripts(mutation);

    // For each mutation observation in turn:
    for (val mutationObservation : mutationObservations) {

      /**
       * Donor: {@code mutation.ssm_occurrence.$.donor}(s).
       */

      // Embed donor
      val mutationObservationDonorId = getObservationDonorId(mutationObservation);
      val mutationObservationDonor = context.getDonor(mutationObservationDonorId);
      setObservationDonor(mutationObservation, mutationObservationDonor);

      /**
       * Project: {@code mutation.ssm_occurrence.$.project}(s).
       */

      // Embed project
      val mutationObservationProjectId = getDonorProjectId(mutationObservationDonor);
      val mutationObservationProject = context.getProject(mutationObservationProjectId);
      setObservationProject(mutationObservation, mutationObservationProject);

      /**
       * Observation: {@code mutation.ssm_occurrence.$}(s).
       */

      // Add to {@code mutation.ssm_occurrence}
      val mutationSsmOccurrence = createMutationSsmOccurrence(
          mutationObservation,
          mutationObservationDonor,
          mutationObservationProject);

      mutationSsmOccurrences.add(mutationSsmOccurrence);

      // For each consequence of the current mutation observation:
      val mutationObservationConsequences = getObservationConsequences(mutationObservation);
      for (val mutationObservationConsequence : mutationObservationConsequences) {

        /**
         * Consequence:
         */

        val consequence = (ObjectNode) mutationObservationConsequence;

        // Get gene associated with consequence: {@code consequence._gene_id}
        val consequenceGeneId =
            firstNonNull(getObservationConsequenceGeneId(consequence), FAKE_GENE_ID);
        val consequenceGene =
            isFakeGeneId(consequenceGeneId) ? createFakeGene() : context.getGene(consequenceGeneId).deepCopy();

        // Get gene transcript associated with consequence: {@code consequence._transcript_id}
        val consequenceGeneTranscriptId =
            firstNonNull(getObservationConsequenceTranscriptId(consequence), FAKE_TRANSCRIPT_ID);
        val consequenceGeneTranscript = findGeneTranscript(consequenceGene, consequenceGeneTranscriptId);

        // Index transcript by transcript id
        mutationTranscriptMap.put(
            consequenceGeneTranscriptId, consequenceGeneTranscript);
        // Index gene by transcript id
        mutationTranscriptGeneMap.put(
            consequenceGeneTranscriptId, consequenceGene);
        // Index consequence by transcript id
        mutationTranscriptConsequenceMap.put(
            consequenceGeneTranscriptId, consequence);
      }
    }

    /**
     * Transcripts: {@code mutation.transcript}(s).
     */

    // Add all affected transcripts
    val transcriptIds = mutationTranscriptMap.keySet();
    for (val transcriptId : transcriptIds) {
      // Resolve or create fake transcript
      val transcript = Fakes.isFakeTranscriptId(transcriptId) ?
          Fakes.createFakeTranscript() : mutationTranscriptMap.get(transcriptId);

      // Resolve children
      val gene = mutationTranscriptGeneMap.get(transcriptId);
      val consequence = mutationTranscriptConsequenceMap.get(transcriptId);

      // Add transcript to mutation
      val mutationTranscript = createMutationTranscript(transcript, gene, consequence);
      mutationTranscripts.add(mutationTranscript);
    }

    // Result
    val document = new Document(context.getType(), mutationId, mutation);

    // Summarize
    SUMMARY_CALLBACK.call(document);

    return document;
  }

  private static ObjectNode findGeneTranscript(ObjectNode gene, String transcriptId) {
    val geneTranscripts = getGeneTranscripts(gene);

    for (val element : geneTranscripts) {
      val geneTranscript = (ObjectNode) element;
      val geneTranscriptId = getTranscriptId(geneTranscript);

      if (geneTranscriptId.equals(transcriptId)) {
        return geneTranscript;
      }
    }

    return null;
  }

  private static ObjectNode createMutationSsmOccurrence(ObjectNode observation, ObjectNode donor, ObjectNode project) {
    ObjectNode ssmOccurrence = observation.deepCopy();

    // Remove unneeded fields
    // lombok: can't use ImmutableList.of with val here
    List<String> internalFields = newArrayList(OBSERVATION_ID, OBSERVATION_TYPE);
    List<String> migratedFields =
        newArrayList(
            OBSERVATION_MUTATION_ID,
            OBSERVATION_CONSEQUENCE_TYPES,
            OBSERVATION_CONSEQUENCES,
            OBSERVATION_FUNCTIONAL_IMPACT_PREDICTION_SUMMARY,
            OBSERVATION_IS_ANNOTATED);
    val keyFields = BusinessKeys.MUTATION;
    ssmOccurrence
        .remove(internalFields)
        .remove(migratedFields)
        .remove(keyFields);

    // Add entities
    ssmOccurrence.put(MUTATION_OBSERVATION_DONOR, donor);
    ssmOccurrence.put(MUTATION_OBSERVATION_PROJECT, project);

    return ssmOccurrence;
  }

  private static ObjectNode createMutationTranscript(ObjectNode transcript, ObjectNode gene, ObjectNode consequence) {
    // Copy and default (empty if missing)
    consequence = defaultObject(consequence).deepCopy();
    gene = defaultObject(gene).deepCopy();

    // Default missing values
    defaultMissing(consequence, CONSEQUENCE_AA_MUTATION);
    defaultMissing(gene, GENE_ID);
    defaultMissing(gene, GENE_SYMBOL);

    // Prune
    gene.remove(GENE_TRANSCRIPTS);

    // Remove
    val impactPredictionSummary =
        consequence.remove(OBSERVATION_CONSEQUENCES_CONSEQUENCE_FUNCTIONAL_IMPACT_PREDICTION_SUMMARY);
    val impactPrediction = consequence.remove(OBSERVATION_CONSEQUENCES_CONSEQUENCE_FUNCTIONAL_IMPACT_PREDICTION);

    // Embed
    transcript.put(MUTATION_TRANSCRIPTS_CONSEQUENCE, consequence);
    transcript.put(MUTATION_TRANSCRIPTS_GENE, gene);
    transcript.put(MUTATION_TRANSCRIPTS_FUNCTIONAL_IMPACT_PREDICTION_SUMMARY, impactPredictionSummary);
    transcript.put(MUTATION_TRANSCRIPTS_FUNCTIONAL_IMPACT_PREDICTION, impactPrediction);

    return transcript;
  }

  private static TreeMap<String, ObjectNode> newTreeMap() {
    return Maps.<String, ObjectNode> newTreeMap();
  }

}
