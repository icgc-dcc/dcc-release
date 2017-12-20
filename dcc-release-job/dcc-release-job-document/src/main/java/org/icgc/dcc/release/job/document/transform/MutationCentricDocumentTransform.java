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
package org.icgc.dcc.release.job.document.transform;

import static com.google.common.base.Objects.firstNonNull;
import static com.google.common.collect.Lists.newArrayList;
import static org.icgc.dcc.common.core.model.FieldNames.CONSEQUENCE_AA_MUTATION;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_PROJECT_ID;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_SYMBOL;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_TRANSCRIPTS;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_OBSERVATION_DONOR;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_OBSERVATION_PROJECT;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_TRANSCRIPTS_CONSEQUENCE;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_TRANSCRIPTS_FUNCTIONAL_IMPACT_PREDICTION_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_TRANSCRIPTS_GENE;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES_CONSEQUENCE_FUNCTIONAL_IMPACT_PREDICTION;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES_CONSEQUENCE_FUNCTIONAL_IMPACT_PREDICTION_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_DONOR_ID;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_FUNCTIONAL_IMPACT_PREDICTION_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_ID;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_IS_ANNOTATED;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_MUTATION_ID;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_TYPE;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.*;
import static org.icgc.dcc.release.job.document.util.Fakes.FAKE_GENE_ID;
import static org.icgc.dcc.release.job.document.util.Fakes.FAKE_TRANSCRIPT_ID;
import static org.icgc.dcc.release.job.document.util.Fakes.createFakeGene;
import static org.icgc.dcc.release.job.document.util.Fakes.isFakeGeneId;
import static org.icgc.dcc.release.job.document.util.JsonNodes.defaultMissing;
import static org.icgc.dcc.release.job.document.util.JsonNodes.defaultObject;

import java.util.List;
import java.util.TreeMap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.Function;
import org.icgc.dcc.common.core.model.BusinessKeys;
import org.icgc.dcc.release.core.document.Document;
import org.icgc.dcc.release.job.document.context.MutationCentricDocumentContext;
import org.icgc.dcc.release.job.document.core.DocumentCallback;
import org.icgc.dcc.release.job.document.core.DocumentContext;
import org.icgc.dcc.release.job.document.core.DocumentJobContext;
import org.icgc.dcc.release.job.document.core.DocumentTransform;
import org.icgc.dcc.release.job.document.util.Fakes;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.icgc.dcc.release.job.document.util.JsonNodes;
import scala.Tuple2;

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
@Slf4j
@RequiredArgsConstructor
public class MutationCentricDocumentTransform extends AbstractCentricDocumentTransform implements
    Function<Tuple2<String, Tuple2<ObjectNode, Optional<Iterable<ObjectNode>>>>, Document> {

  @NonNull
  private final DocumentJobContext documentJobContext;
  private static final DocumentCallback SUMMARY_CALLBACK = new MutationCentricSummaryCallback();

  @Override
  public Document call(Tuple2<String, Tuple2<ObjectNode, Optional<Iterable<ObjectNode>>>> tuple) throws Exception {
    val mutation = tuple._2._1;
    val observations = tuple._2._2;
    val mutationId = getMutationId(mutation);
    val documentContext = new MutationCentricDocumentContext(mutationId, documentJobContext, observations);

    return transformDocument(mutation, documentContext);
  }

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
      setMutationObservationProject(mutationObservation, mutationObservationProject);

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
    for (val transcriptEntry : mutationTranscriptMap.entrySet()) {
      val transcriptId = transcriptEntry.getKey();
      // Resolve or create fake transcript
      val transcript =
          Fakes.isFakeTranscriptId(transcriptId) ? Fakes.createFakeTranscript() : transcriptEntry.getValue();

      // Resolve children
      val gene = mutationTranscriptGeneMap.get(transcriptId);
      val consequence = mutationTranscriptConsequenceMap.get(transcriptId);

      // Add transcript to mutation
      val mutationTranscript = createMutationTranscript(transcript, gene, consequence);
      mutationTranscripts.add(mutationTranscript);
    }

    // Attach annotation data
    val annotationId = getMutationVariantAnnotationId(mutation);
    val clinvar =  context.getClinvar(annotationId);
    val civic =  context.getCivic(annotationId);
    mutation = attachVariantAnnotationData(mutation, clinvar, civic);

    // Temporary debugging
    log.info("TEST LOG OUTPUT");
    if (mutationId.equals("MU62030")) {
      log.info("Annotation ID: " + annotationId);
//      log.info("Clinvar: " + clinvar.toString());
//      log.info("Civic: " + civic.toString());
      log.info("Mutation: " + mutation.toString());
    }

    // Result
    val document = new Document(context.getType(), mutationId, mutation);

    // Summarize
    SUMMARY_CALLBACK.call(document);

    return document;
  }

  private static ObjectNode attachVariantAnnotationData(ObjectNode mutation, ObjectNode clinvar, Iterable<ObjectNode> civic) {

    // ObjectMapper mapper used to create new nodes
    ObjectMapper mapper = new ObjectMapper();

    // Attach empty nodes used later on
    val external_db_ids = mapper.createObjectNode();
    val clinical_significance = mapper.createObjectNode();
    val clinical_evidence = mapper.createObjectNode();
    mutation.set("external_db_ids", external_db_ids);
    mutation.set("clinical_significance", clinical_significance);
    mutation.set("clinical_evidence", clinical_evidence);

    // TEMP
    val mutationId = getMutationId(mutation);

    // If there is clinvar data pass it through otherwise don't and get defaults
    if (clinvar == null) {
      attachClinvarData(mutation);
      if (mutationId.equals("MU62030")) {
        log.info("MU62030 will be clinvar NULL"); // TEMP
      }
    } else {
      attachClinvarData(mutation, clinvar);
      if (mutationId.equals("MU62030")) {
        log.info("MU62030 will have clinvar data"); // TEMP
      }
    }

    // If there is civic data pass it through otherwise don't and get defaults
    if (civic == null) {
      attachCivicData(mutation);
      if (mutationId.equals("MU62030")) {
        log.info("MU62030 will be civic NULL"); // TEMP
      }
    } else {
      attachCivicData(mutation, civic);
      if (mutationId.equals("MU62030")) {
        log.info("MU62030 will have civic data"); // TEMP
      }
    }

    // Finally return the mutation with annotation data now attached
    return mutation;
  }

  /**
   * Attached default (empty/null) values if no clinvar data is passed in
   * @param mutation - object node
   * @return mutation with empty clinvar fields
   */
  private static ObjectNode attachClinvarData(ObjectNode mutation) {
    ((ObjectNode)mutation.get("external_db_ids")).set("clinvar", null);
    ((ObjectNode)mutation.get("clinical_significance")).set("clinvar", null);

    return mutation;
  }

  /**
   * Attaches passed in clinvar data to mutation
   * @param mutation - object node
   * @param clinvar object node to populate clinvar data
   * @return mutation with complete clinvar fields
   */
  private static ObjectNode attachClinvarData(ObjectNode mutation, ObjectNode clinvar) {
    // Clinvar field extraction
    val clinvarId = clinvar.get("clinvarID");

    // Set fields
    ((ObjectNode)mutation.get("external_db_ids")).set("clinvar", clinvarId);
    ((ObjectNode)mutation.get("clinical_significance")).set("clinvar", clinvar);

    return mutation;
  }

  /**
   * Attached default (empty/null) values if no civic data is passed in
   * @param mutation - object node
   * @return mutation with empty civic fields
   */
  private static ObjectNode attachCivicData(ObjectNode mutation) {
    ((ObjectNode)mutation.get("external_db_ids")).set("civic", null);
    ((ObjectNode)mutation.get("clinical_evidence")).set("civic", null);
    mutation.set("description", null);

    return mutation;
  }

  /**
   * Attaches passed in civic data to mutation
   * @param mutation - object node
   * @param civic iterable to populate civic data
   * @return mutation with complete civic data
   */
  private static ObjectNode attachCivicData(ObjectNode mutation, Iterable<ObjectNode> civic) {
    // Civic field extraction
    val oneCivic = Iterables.get(civic, 0);

    val civicId = Integer.parseInt(oneCivic.get("civicID").textValue());
    val description = oneCivic.get("variantSummary");

    // Set fields
    ((ObjectNode)mutation.get("external_db_ids")).put("civic", civicId);
    mutation.set("description", description);

    ObjectMapper mapper = new ObjectMapper();

    ArrayNode civicData = mapper.createArrayNode();
    civic.forEach(civicData::add);
    ((ObjectNode)mutation.get("clinical_evidence")).set("civic", civicData);

    return mutation;
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
            OBSERVATION_DONOR_ID,
            OBSERVATION_MUTATION_ID,
            OBSERVATION_CONSEQUENCES,
            OBSERVATION_FUNCTIONAL_IMPACT_PREDICTION_SUMMARY,
            OBSERVATION_IS_ANNOTATED);
    val keyFields = BusinessKeys.MUTATION;
    ssmOccurrence
        .remove(internalFields)
        .remove(migratedFields)
        .remove(keyFields);

    // Trim donor
    val donorCopy = donor.deepCopy();
    donorCopy.remove(DONOR_PROJECT_ID);

    // Add entities
    ssmOccurrence.set(MUTATION_OBSERVATION_DONOR, donorCopy);
    ssmOccurrence.set(MUTATION_OBSERVATION_PROJECT, project);

    return ssmOccurrence;
  }

  private static ObjectNode createMutationTranscript(ObjectNode transcript, ObjectNode gene, ObjectNode consequence) {
    // Copy and default (empty if missing)
    val consequenceCopy = defaultObject(consequence).deepCopy();
    val geneCopy = defaultObject(gene).deepCopy();

    // Default missing values
    defaultMissing(consequenceCopy, CONSEQUENCE_AA_MUTATION);
    defaultMissing(geneCopy, GENE_ID);
    defaultMissing(geneCopy, GENE_SYMBOL);

    // Prune
    geneCopy.remove(GENE_TRANSCRIPTS);

    // Remove
    val impactPredictionSummary =
        consequenceCopy.remove(OBSERVATION_CONSEQUENCES_CONSEQUENCE_FUNCTIONAL_IMPACT_PREDICTION_SUMMARY);
    consequenceCopy.remove(OBSERVATION_CONSEQUENCES_CONSEQUENCE_FUNCTIONAL_IMPACT_PREDICTION);

    // Embed
    transcript.set(MUTATION_TRANSCRIPTS_CONSEQUENCE, consequenceCopy);
    transcript.set(MUTATION_TRANSCRIPTS_GENE, geneCopy);
    transcript.set(MUTATION_TRANSCRIPTS_FUNCTIONAL_IMPACT_PREDICTION_SUMMARY, impactPredictionSummary);

    return transcript;
  }

  private static TreeMap<String, ObjectNode> newTreeMap() {
    return Maps.<String, ObjectNode> newTreeMap();
  }

}
