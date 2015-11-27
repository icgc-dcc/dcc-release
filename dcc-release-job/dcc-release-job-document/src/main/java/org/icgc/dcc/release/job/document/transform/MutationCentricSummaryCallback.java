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

import static java.util.Collections.reverseOrder;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_CONSEQUENCE_TYPES;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_FUNCTIONAL_IMPACT_PREDICTION_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_IS_ANNOTATED;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_PLATFORM;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_SEQUENCING_STRATEGY;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_SUMMARY_AFFECTED_DONOR_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_SUMMARY_AFFECTED_PROJECT_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_SUMMARY_AFFECTED_PROJECT_IDS;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_SUMMARY_TESTED_DONOR_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_VALIDATION_STATUS;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_VERIFICATION_STATUS;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getConsequenceType;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getDonorId;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getMutationObservationDonor;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getMutationObservationProject;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getMutationOccurrences;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getMutationTranscriptConsequence;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getMutationTranscriptImpactPredictionSummary;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getMutationTranscripts;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getObservationIsAnnotated;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getObservationPlatform;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getObservationSequenceStrategy;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getObservationValidationStatus;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getObservationVerificationStatus;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getOccurrenceObservations;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getProjectId;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getProjectTestedDonorSsmCount;
import static org.icgc.dcc.release.job.document.util.JsonNodes.addAll;

import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

import lombok.val;

import org.icgc.dcc.common.core.fi.CompositeImpactCategory;
import org.icgc.dcc.common.core.model.ConsequenceType;
import org.icgc.dcc.release.job.document.core.Document;
import org.icgc.dcc.release.job.document.core.DocumentCallback;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class MutationCentricSummaryCallback implements DocumentCallback {

  @Override
  public void call(Document document) {
    // Mutation with observations
    ObjectNode mutation = document.getSource();

    val mutationOccurrences = getMutationOccurrences(mutation);

    // Flags
    val isAnnotated = new AtomicReference<String>();

    // Distinct
    val donorIds = newTreeSet();
    val projectIds = newTreeSet();
    val platforms = newTreeSet();
    val validationStatuses = newTreeSet();
    val verificationStatuses = newTreeSet();
    val sequencingStrategies = newTreeSet();

    // Counts
    val projectsDonorsTested = Maps.<String, Integer> newHashMap();

    for (val mutationOccurrence : mutationOccurrences) {

      val observations = getOccurrenceObservations((ObjectNode) mutationOccurrence);

      for (val mutationObservation : observations) {
        // Collect platforms
        val platform = getObservationPlatform(mutationObservation);
        if (platform != null) {
          platforms.add(platform);
        }

        // Collect validation statuses
        val validationStatus = getObservationValidationStatus(mutationObservation);
        if (validationStatus != null) {
          validationStatuses.add(validationStatus);
        }

        // Collect verification statuses
        val verificationStatus = getObservationVerificationStatus(mutationObservation);
        if (verificationStatus != null) {
          verificationStatuses.add(verificationStatus);
        }

        // Collection sequencing strategy
        val sequencingStrategy = getObservationSequenceStrategy(mutationObservation);
        if (sequencingStrategy != null) {
          sequencingStrategies.add(sequencingStrategy);
        }

        // Collect annotated flag
        isAnnotated.set(getObservationIsAnnotated(mutationObservation));
      }

      // Collect donor id
      val donor = getMutationObservationDonor((ObjectNode) mutationOccurrence);
      val donorId = getDonorId(donor);
      donorIds.add(donorId);

      // Collect project id
      val project = getMutationObservationProject((ObjectNode) mutationOccurrence);
      val projectId = getProjectId(project);
      projectIds.add(projectId);

      // Collect project donor (ssm) tested count
      val projectDonorsTested = getProjectTestedDonorSsmCount(project);
      projectsDonorsTested.put(projectId, projectDonorsTested);
    }

    /**
     * Is Annotated? : {@code mutation.is_annotated}.
     */

    mutation.put(MUTATION_IS_ANNOTATED, isAnnotated.get());

    /**
     * Platforms: {@code mutation.plaform}(s).
     */

    // Add unique value to top level for faceting
    addAll(mutation.withArray(MUTATION_PLATFORM), platforms);

    /**
     * Verification Statuses: {@code mutation.verification_status}(s).
     */

    addAll(mutation.withArray(MUTATION_VALIDATION_STATUS), validationStatuses);

    /**
     * Validation Statuses: {@code mutation.validation_status}(s).
     */

    addAll(mutation.withArray(MUTATION_VERIFICATION_STATUS), verificationStatuses);

    // Add unique sequencing strategies to mutation level
    addAll(mutation.withArray(MUTATION_SEQUENCING_STRATEGY), sequencingStrategies);

    /**
     * Consequence Types: {@code mutation.consequence_type}(s).
     */

    val transcripts = getMutationTranscripts(mutation);
    val impactCategories = getPrioritizedImpactCategories(transcripts);
    val consequenceTypes = getPrioritizedConsequenceTypes(transcripts);

    // Add unique value to top level for faceting
    mutation.putPOJO(MUTATION_CONSEQUENCE_TYPES, consequenceTypes);
    mutation.putPOJO(MUTATION_FUNCTIONAL_IMPACT_PREDICTION_SUMMARY, impactCategories);

    /**
     * Summary: {@code mutation._summary}.
     */

    // Get the total tested from all the projects
    int testedDonorCount = 0;
    for (val count : projectsDonorsTested.values()) {
      testedDonorCount += count;
    }

    // Create summary
    val mutationSummary = mutation.objectNode();
    mutationSummary.put(MUTATION_SUMMARY_AFFECTED_DONOR_COUNT, donorIds.size());
    mutationSummary.put(MUTATION_SUMMARY_AFFECTED_PROJECT_COUNT, projectIds.size());
    mutationSummary.put(MUTATION_SUMMARY_TESTED_DONOR_COUNT, testedDonorCount);
    addAll(mutationSummary.withArray(MUTATION_SUMMARY_AFFECTED_PROJECT_IDS), projectIds);

    mutation.put(MUTATION_SUMMARY, mutationSummary);
  }

  private static Set<CompositeImpactCategory> getPrioritizedImpactCategories(ArrayNode transcripts) {
    val impactCategories = Sets.<CompositeImpactCategory> newTreeSet(reverseOrder());
    for (val transcript : transcripts) {
      val id = getMutationTranscriptImpactPredictionSummary(transcript);

      if (id != null) {
        val category = CompositeImpactCategory.byId(id);
        impactCategories.add(category);
      }
    }

    return impactCategories;
  }

  private static Set<ConsequenceType> getPrioritizedConsequenceTypes(ArrayNode transcripts) {
    val consequenceTypes = Sets.<ConsequenceType> newTreeSet(reverseOrder());
    for (val transcript : transcripts) {
      val consequence = getMutationTranscriptConsequence((ObjectNode) transcript);
      val id = getConsequenceType(consequence);

      if (id != null) {
        val consequenceType = ConsequenceType.byId(id);
        consequenceTypes.add(consequenceType);
      }
    }

    return consequenceTypes;
  }

  private static TreeSet<String> newTreeSet() {
    return Sets.<String> newTreeSet();
  }

}