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

import static com.google.common.collect.ImmutableList.of;
import static com.google.common.collect.Iterables.size;
import static org.icgc.dcc.common.core.model.FeatureTypes.FeatureType.SSM_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_DONORS;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_DONOR_PROJECT;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_DONOR_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_PROJECTS;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_SUMMARY_AFFECTED_DONOR_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_SUMMARY_AFFECTED_PROJECT_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_SUMMARY_AFFECTED_TRANSCRIPT_IDS;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_SUMMARY_TOTAL_MUTATION_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_SUMMARY_UNIQUE_MUTATION_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES_TRANSCRIPT_ID;
import static org.icgc.dcc.common.core.model.FieldNames.PROJECT_DISPLAY_NAME;
import static org.icgc.dcc.common.core.model.FieldNames.PROJECT_ID;
import static org.icgc.dcc.common.core.model.FieldNames.PROJECT_PRIMARY_SITE;
import static org.icgc.dcc.common.core.model.FieldNames.PROJECT_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.TOTAL_DONOR_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.getTestedTypeCountFieldName;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getDonorProjectId;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getGeneDonorId;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getGeneDonors;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getGeneId;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getGeneProjectId;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getGeneProjects;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getObservationDonorId;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getObservationMutationId;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getObservationType;
import static org.icgc.dcc.release.job.document.util.JsonNodes.addAll;
import static org.icgc.dcc.release.job.document.util.JsonNodes.isEmpty;
import static org.icgc.dcc.release.job.document.util.JsonNodes.normalizeTextValue;

import java.util.List;
import java.util.Set;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.spark.api.java.function.Function;
import org.icgc.dcc.common.core.model.FeatureTypes.FeatureType;
import org.icgc.dcc.release.core.document.Document;
import org.icgc.dcc.release.job.document.context.GeneCentricDocumentContext;
import org.icgc.dcc.release.job.document.core.DocumentContext;
import org.icgc.dcc.release.job.document.core.DocumentJobContext;
import org.icgc.dcc.release.job.document.core.DocumentTransform;
import org.icgc.dcc.release.job.document.util.Fakes;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

/**
 * {@link DocumentTransform} implementation that creates a nested gene-centric document.
 */
@RequiredArgsConstructor
public class GeneCentricDocumentTransform extends AbstractCentricDocumentTransform implements
    Function<Tuple2<String, Tuple2<ObjectNode, Optional<Iterable<ObjectNode>>>>, Document> {

  private final DocumentJobContext indexJobContext;
  private static final List<String> DONOR_PROJECT_FIELD_NAMES =
      of(PROJECT_ID, PROJECT_DISPLAY_NAME, PROJECT_PRIMARY_SITE);

  @Override
  public Document call(Tuple2<String, Tuple2<ObjectNode, Optional<Iterable<ObjectNode>>>> tuple) throws Exception {
    val gene = tuple._2._1;
    val observations = tuple._2._2;
    val geneId = getGeneId(gene);
    val documentContext = new GeneCentricDocumentContext(geneId, indexJobContext, observations);

    return transformDocument(gene, documentContext);
  }

  @Override
  public Document transformDocument(@NonNull ObjectNode gene, @NonNull DocumentContext context) {
    // Index observations by donor id
    val geneId = getGeneId(gene);
    val geneObservations = context.getObservationsByGeneId(geneId);
    val geneDonorsObservations = indexGeneDonorsObservations(geneObservations);

    /**
     * Donors: {@code gene.donors}(s).
     */

    // Nest
    val geneDonors = getGeneDonors(gene);
    for (val geneDonorId : geneDonorsObservations.keySet()) {
      val donor = context.getDonor(geneDonorId).deepCopy();
      val donorProjectId = getDonorProjectId(donor);
      val donorProject = createDonorProject(context.getProject(donorProjectId).deepCopy());

      // Extract donor non-feature type summary values for merging
      ObjectNode donorSummary = (ObjectNode) donor.remove(DONOR_SUMMARY);
      if (donorSummary != null) {
        for (val featureType : FeatureType.values()) {
          donorSummary.remove(featureType.getSummaryFieldName());
        }
      } else {
        donorSummary = donor.objectNode();
      }

      // Find gene donor and remove summary for merging
      val geneDonor = findGeneDonor(gene, geneDonors, geneDonorId);
      ObjectNode geneDonorSummary = (ObjectNode) geneDonor.remove(GENE_DONOR_SUMMARY);
      if (geneDonorSummary == null) {
        geneDonorSummary = geneDonor.objectNode();
      }

      // Merge
      geneDonorSummary.setAll(donorSummary);
      geneDonor.setAll(donor);
      geneDonor.set(GENE_DONOR_SUMMARY, geneDonorSummary);

      // Add donor-project info
      geneDonor.set(GENE_DONOR_PROJECT, donorProject);

      // Merge
      val geneDonorObservations = geneDonorsObservations.get(geneDonorId);
      val geneDonorTree = createGeneDonorTree(gene, donor, geneDonorObservations);
      geneDonor.setAll(geneDonorTree);
    }

    if (isEmpty(geneDonors)) {
      // Ensure arrays are present with at least 1 element
      geneDonors.add(Fakes.createPlaceholder());
    }

    /**
     * Projects: {@code gene.projects}(s).
     */

    val geneProjects = getGeneProjects(gene);
    for (val value : geneProjects) {
      val geneProject = (ObjectNode) value;
      val geneProjectId = getGeneProjectId(geneProject);
      val project = context.getProject(geneProjectId).deepCopy();

      // Book keeping
      String totalSsmTestedDonorCountFieldName = getTestedTypeCountFieldName(SSM_TYPE);

      // Filter and cache
      val projectSummary = project.remove(PROJECT_SUMMARY);
      int totalDonorCount = projectSummary.path(TOTAL_DONOR_COUNT).intValue();
      int totalSsmTestedDonorCount = projectSummary.path(totalSsmTestedDonorCountFieldName).intValue();

      // Merge
      geneProject.setAll(project);
      geneProject.with(PROJECT_SUMMARY)
          .put(TOTAL_DONOR_COUNT, totalDonorCount)
          .put(totalSsmTestedDonorCountFieldName, totalSsmTestedDonorCount);
    }

    /**
     * Summary: {@code gene._summary}.
     */

    val summary = gene.with(GENE_SUMMARY);

    val affectedDonorCount = getAffectedDonorCount(gene);
    summary.put(GENE_SUMMARY_AFFECTED_DONOR_COUNT, affectedDonorCount);

    val affectedProjectCount = getAffectedProjectCount(gene);
    summary.put(GENE_SUMMARY_AFFECTED_PROJECT_COUNT, affectedProjectCount);

    addAll(summary.withArray(GENE_SUMMARY_AFFECTED_TRANSCRIPT_IDS),
        getAffectedTranscriptIds(geneDonorsObservations.values()));

    summary.put(GENE_SUMMARY_UNIQUE_MUTATION_COUNT, getUniqueMutationCount(geneDonorsObservations.values()));
    summary.put(GENE_SUMMARY_TOTAL_MUTATION_COUNT, getTotalMutationCount(geneDonorsObservations.values()));

    return new Document(context.getType(), geneId, gene);
  }

  private static ObjectNode createGeneDonorTree(ObjectNode gene, ObjectNode geneDonor,
      Iterable<ObjectNode> geneDonorObservations) {

    // Process each observation associated with the current donor
    for (val geneDonorObservation : geneDonorObservations) {
      // Add to feature type array (e.g. "ssm")
      val observationType = getObservationType(geneDonorObservation);
      val array = geneDonor.withArray(observationType);

      // Remove unrelated gene consequences and aggregate
      transformGeneObservationConsequences(gene, geneDonorObservation);

      array.add(geneDonorObservation);
    }

    return geneDonor;
  }

  private static ObjectNode findGeneDonor(Object gene, ArrayNode geneDonors, Object geneDonorId) {
    for (val value : geneDonors) {
      val geneDonor = (ObjectNode) value;

      // Is it an id match?
      val idMatch = getGeneDonorId(geneDonor).equals(geneDonorId);
      if (idMatch) {
        return geneDonor;
      }
    }

    throw new RuntimeException("No gene donor summary found for donor id '" + geneDonorId + "' and gene " + gene);
  }

  private static int getAffectedDonorCount(ObjectNode gene) {
    val geneDonors = gene.withArray(GENE_DONORS);
    int rawAffectedDonorCount = geneDonors.size();

    int placeholderDonorCount = 0;
    for (val geneDonor : geneDonors) {
      if (Fakes.isPlaceholder(geneDonor)) {
        placeholderDonorCount++;
      }
    }

    // We don't want to include placeholders as they are purely synthetic.
    // See https://jira.oicr.on.ca/browse/DCC-2506
    val realAffectedDonorCount = rawAffectedDonorCount - placeholderDonorCount;

    return realAffectedDonorCount;
  }

  private static int getAffectedProjectCount(ObjectNode gene) {
    ArrayNode projects = gene.withArray(GENE_PROJECTS);
    int affectedProjectCount = projects.size();

    return affectedProjectCount;
  }

  private static Set<String> getAffectedTranscriptIds(@NonNull Iterable<ObjectNode> geneObservations) {
    val transcriptIds = Sets.<String> newLinkedHashSet();

    for (val geneObservation : geneObservations) {
      // Add affected transcripts
      ArrayNode consequences = (ArrayNode) geneObservation.get(OBSERVATION_CONSEQUENCES);
      if (consequences != null) {
        for (JsonNode consequence : consequences) {
          val transcriptId = normalizeTextValue(consequence, OBSERVATION_CONSEQUENCES_TRANSCRIPT_ID);
          if (transcriptId != null) {
            transcriptIds.add(transcriptId);
          }
        }
      }
    }

    return transcriptIds;
  }

  private static int getTotalMutationCount(@NonNull Iterable<ObjectNode> geneObservations) {
    return size(geneObservations);
  }

  private static int getUniqueMutationCount(@NonNull Iterable<ObjectNode> geneObservations) {
    val mutationIds = Sets.<String> newHashSet();
    for (val geneObservation : geneObservations) {
      mutationIds.add(getObservationMutationId(geneObservation));
    }

    return mutationIds.size();
  }

  private static ObjectNode createDonorProject(@NonNull ObjectNode project) {
    // Create light-weight project information for donor sub-document
    ObjectNode donorProject = project.objectNode();

    for (String fieldName : DONOR_PROJECT_FIELD_NAMES) {
      donorProject.set(fieldName, project.get(fieldName));
    }

    return donorProject;
  }

  private static Multimap<String, ObjectNode> indexGeneDonorsObservations(@NonNull Iterable<ObjectNode> observations) {
    // Set and multi-semantics are required
    val index = ImmutableSetMultimap.<String, ObjectNode> builder();
    for (val observation : observations) {
      val donorId = getObservationDonorId(observation);

      index.put(donorId, observation);
    }

    return index.build();
  }

}
