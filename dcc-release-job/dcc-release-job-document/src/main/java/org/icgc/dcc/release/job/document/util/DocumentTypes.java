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
package org.icgc.dcc.release.job.document.util;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.of;
import static lombok.AccessLevel.PRIVATE;

import java.util.Collection;
import java.util.Map;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.release.core.document.DocumentType;
import org.icgc.dcc.release.job.document.model.BroadcastType;
import org.icgc.dcc.release.job.document.model.CollectionFields;
import org.icgc.dcc.release.job.document.model.DocumentFields;
import org.icgc.dcc.release.job.document.model.DocumentTypeAttributes;
import org.icgc.dcc.release.job.document.task.DiagramDocumentTask;
import org.icgc.dcc.release.job.document.task.DonorCentricDocumentTask;
import org.icgc.dcc.release.job.document.task.DonorDocumentTask;
import org.icgc.dcc.release.job.document.task.DonorTextDocumentTask;
import org.icgc.dcc.release.job.document.task.GeneCentricDocumentTask;
import org.icgc.dcc.release.job.document.task.GeneDocumentTask;
import org.icgc.dcc.release.job.document.task.GeneSetDocumentTask;
import org.icgc.dcc.release.job.document.task.GeneSetTextDocumentTask;
import org.icgc.dcc.release.job.document.task.GeneTextDocumentTask;
import org.icgc.dcc.release.job.document.task.MutationCentricDocumentTask;
import org.icgc.dcc.release.job.document.task.MutationTextDocumentTask;
import org.icgc.dcc.release.job.document.task.ObservationCentricDocumentTask;
import org.icgc.dcc.release.job.document.task.ProjectDocumentTask;
import org.icgc.dcc.release.job.document.task.ProjectTextDocumentTask;
import org.icgc.dcc.release.job.document.task.ReleaseDocumentTask;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

@NoArgsConstructor(access = PRIVATE)
public final class DocumentTypes {

  private static final Map<DocumentType, DocumentTypeAttributes> DOCUMENT_TYPES = defineDocumentTypes();

  public static String getIndexClassName(@NonNull DocumentType documentType) {
    return DOCUMENT_TYPES.get(documentType).indexClassName();
  }

  public static Collection<BroadcastType> getBroadcastDependencies(@NonNull DocumentType documentType) {
    return DOCUMENT_TYPES.get(documentType).broadcastDependencies();
  }

  public static DocumentFields getFields(@NonNull DocumentType documentType) {
    return DOCUMENT_TYPES.get(documentType).fields();
  }

  private static Map<DocumentType, DocumentTypeAttributes> defineDocumentTypes() {
    val documentTypes =
        ImmutableMap
            .<DocumentType, DocumentTypeAttributes> builder()
            .put(DocumentType.DIAGRAM_TYPE, attributes().indexClassName(DiagramDocumentTask.class.getName()))
            .put(DocumentType.RELEASE_TYPE, attributes().indexClassName(ReleaseDocumentTask.class.getName()))

            .put(DocumentType.GENE_SET_TYPE,
                attributes()
                    .indexClassName(GeneSetDocumentTask.class.getName())
                    .fields(fields()
                        .geneFields(
                            geneFields()
                                .includedFields(
                                    "_gene_id",

                                    // Gene sets
                                    "sets.id",
                                    "sets.type",

                                    "project._project_id",
                                    "donor")
                        )
                        .projectFields(
                            projectFields()
                                .includedFields(
                                    "_project_id",
                                    "project_name",
                                    "primary_site",
                                    "tumour_type",
                                    "tumour_subtype",
                                    "_summary._ssm_tested_donor_count",
                                    "_summary._total_donor_count")
                        )
                    )
            )
            .put(DocumentType.GENE_SET_TEXT_TYPE,
                attributes()
                    .indexClassName(GeneSetTextDocumentTask.class.getName())
                    .fields(fields()
                        .geneSetFields(
                            geneSetFields()
                                .includedFields(
                                    "id",
                                    "name",
                                    "type",
                                    "source",
                                    "go_term.alt_ids")
                        )
                    )
            )

            .put(DocumentType.PROJECT_TYPE, attributes().indexClassName(ProjectDocumentTask.class.getName()))
            .put(DocumentType.PROJECT_TEXT_TYPE,
                attributes()
                    .indexClassName(ProjectTextDocumentTask.class.getName())
                    .fields(fields()
                        .projectFields(
                            projectFields()
                                .includedFields(
                                    "_project_id",
                                    "project_name",
                                    "tumour_type",
                                    "tumour_subtype",
                                    "primary_site",
                                    "_summary._state")
                        )
                    )
            )

            .put(DocumentType.DONOR_TYPE,
                attributes()
                    .indexClassName(DonorDocumentTask.class.getName())
                    .broadcastDependencies(ImmutableList.of(BroadcastType.PROJECT))
                    .fields(fields()
                        .donorFields(
                            donorFields()
                                .excludedFields(
                                    "_id",
                                    "gene")
                        )
                    )
            )
            .put(DocumentType.DONOR_TEXT_TYPE,
                attributes()
                    .indexClassName(DonorTextDocumentTask.class.getName())
                    .fields(fields()
                        .donorFields(
                            donorFields()
                                .includedFields(
                                    "_donor_id",
                                    "_project_id",
                                    "donor_id",
                                    "specimen._specimen_id",
                                    "specimen.specimen_id",
                                    "specimen.sample._sample_id",
                                    "specimen.sample.analyzed_sample_id",
                                    "_summary._state")
                        )
                    )
            )
            .put(DocumentType.DONOR_CENTRIC_TYPE,
                attributes()
                    .indexClassName(DonorCentricDocumentTask.class.getName())
                    .broadcastDependencies(ImmutableList.of(BroadcastType.GENE, BroadcastType.PROJECT))
                    .fields(fields()
                        .projectFields(
                            projectFields()
                                .includedFields(
                                    // Primary key
                                    "_project_id",

                                    // Data
                                    "primary_site")
                        )
                        .donorFields(
                            donorFields()
                                .includedFields(
                                    // Primary key
                                    "_donor_id",

                                    // Foreign keys
                                    "_project_id",

                                    // Summary
                                    "gene._gene_id",
                                    "_summary",

                                    // Data
                                    "disease_status_last_followup",
                                    "donor_age_at_diagnosis",
                                    "donor_relapse_type",
                                    "donor_sex",
                                    "donor_survival_time",
                                    "donor_tumour_stage_at_diagnosis",
                                    "donor_vital_status")
                        )
                        .geneFields(
                            geneFields()
                                .includedFields(
                                    // Primary key
                                    "_gene_id",

                                    // Data
                                    "symbol",
                                    "biotype",
                                    "chromosome",
                                    "start",
                                    "end",

                                    // Gene sets
                                    "sets.id",
                                    "sets.type")
                        )
                        .observationFields(
                            observationFields()
                                .includedFields(
                                    // Foreign keys
                                    "_mutation_id",
                                    "_donor_id", // TODO: New from ETL1! Needed due to lack of MongoDB
                                    "consequence._gene_id",

                                    // Data
                                    "consequence.consequence_type",
                                    "consequence.functional_impact_prediction_summary",
                                    "_type",
                                    "mutation_type",
                                    "chromosome",
                                    "chromosome_end",
                                    "chromosome_start",
                                    "observation.platform",
                                    "observation.sequencing_strategy",
                                    "observation.verification_status"
                                )
                        )
                    )
            )

            .put(DocumentType.GENE_TYPE,
                attributes()
                    .indexClassName(GeneDocumentTask.class.getName())
                    .fields(fields()
                        .geneFields(geneFields()
                            .excludedFields("donor")
                        )
                        .observationFields(
                            observationFields()
                                .excludedFields(
                                    "_id",
                                    "_summary",
                                    "project",
                                    "donor")
                        )
                    )
            )
            .put(DocumentType.GENE_TEXT_TYPE,
                attributes()
                    .indexClassName(GeneTextDocumentTask.class.getName())
                    .fields(fields()
                        .geneFields(
                            geneFields()
                                .includedFields(
                                    "_gene_id",
                                    "symbol",
                                    "name",
                                    "synonyms",
                                    "external_db_ids")
                        )
                    )
            )
            .put(DocumentType.GENE_CENTRIC_TYPE,
                attributes()
                    .indexClassName(GeneCentricDocumentTask.class.getName())
                    .broadcastDependencies(ImmutableList.of(BroadcastType.DONOR, BroadcastType.PROJECT))
                    .fields(
                        fields()
                            .projectFields(
                                projectFields()
                                    .includedFields(
                                        "_project_id",
                                        "primary_site"
                                    )
                            )
                            .donorFields(
                                donorFields()
                                    .includedFields(
                                        "_donor_id",
                                        "_project_id",
                                        "_summary._age_at_diagnosis_group",
                                        "_summary._available_data_type",
                                        "_summary._state",
                                        "_summary._studies",
                                        "_summary.experimental_analysis_performed",
                                        "disease_status_last_followup",
                                        "donor_relapse_type",
                                        "donor_sex",
                                        "donor_tumour_stage_at_diagnosis",
                                        "donor_vital_status"
                                    )
                            )
                            .observationFields(
                                observationFields()
                                    .includedFields(
                                        "_mutation_id",
                                        "_donor_id", // Don't index
                                        "_type", // Don't index
                                        "chromosome",
                                        "chromosome_end",
                                        "chromosome_start",
                                        "consequence._gene_id",
                                        "consequence.consequence_type",
                                        "consequence.transcript_affected",
                                        "consequence.functional_impact_prediction_summary",
                                        "mutation_type",
                                        "observation.platform",
                                        "observation.sequencing_strategy",
                                        "observation.verification_status"
                                    )
                            )
                            .geneFields(
                                geneFields()
                                    .excludedFields(
                                        "_id",
                                        "canonical_transcript_id",
                                        "description",
                                        "external_db_ids",
                                        "project",
                                        "sets",
                                        "strand",
                                        "synonyms",
                                        "transcripts"
                                    )
                            )
                    )
            )

            .put(
                DocumentType.OBSERVATION_CENTRIC_TYPE,
                attributes()
                    .indexClassName(ObservationCentricDocumentTask.class.getName())
                    .broadcastDependencies(of(BroadcastType.DONOR, BroadcastType.PROJECT, BroadcastType.GENE))
                    .fields(
                        fields()
                            .projectFields(
                                projectFields()
                                    .includedFields(
                                        "_project_id",
                                        "primary_site")
                            )
                            .donorFields(
                                donorFields()
                                    .includedFields(
                                        "_donor_id",
                                        "_project_id", // FK. Don't index

                                        // Data
                                        "donor_sex",
                                        "donor_tumour_stage_at_diagnosis",
                                        "donor_vital_status",
                                        "disease_status_last_followup",
                                        "donor_relapse_type",
                                        "_summary._age_at_diagnosis_group",
                                        "_summary._available_data_type",
                                        "_summary._studies",
                                        "_summary.experimental_analysis_performed",
                                        "_summary._state")
                            )
                            .geneFields(
                                geneFields()
                                    .includedFields(
                                        "_gene_id",
                                        "biotype",
                                        "go_term.cellular_component",
                                        "go_term.biological_process",
                                        "go_term.molecular_function",
                                        "curated_set",
                                        "chromosome",
                                        "start",
                                        "end",
                                        "pathway")

                            )
                            .observationFields(
                                observationFields()
                                    .excludedFields(
                                        "_id",
                                        "assembly_version",
                                        "mutated_to_allele",
                                        "mutated_from_allele",
                                        "reference_genome_allele",
                                        "chromosome_strand",
                                        "functional_impact_prediction_summary",

                                        // Consequence
                                        "consequence.gene_build_version",
                                        "consequence.transcript_affected",
                                        "consequence.gene_affected",
                                        "consequence._transcript_id",
                                        "consequence.functional_impact_prediction",

                                        // Observation
                                        "observation.analysis_id",
                                        "observation.quality_score",
                                        "observation.verification_platform",
                                        "observation.marking",
                                        "observation.observation_id",
                                        "observation.seq_coverage")
                            )
                    )
            )

            .put(DocumentType.MUTATION_TEXT_TYPE,
                attributes()
                    .indexClassName(MutationTextDocumentTask.class.getName())
                    .broadcastDependencies(ImmutableList.of(BroadcastType.GENE))
                    .fields(fields()
                        .mutationFields(
                            mutationFields()
                                .includedFields(
                                    "_mutation_id",
                                    "mutation",
                                    "chromosome",
                                    "chromosome_start")
                        )

                        .observationFields(
                            observationFields()
                                .includedFields(
                                    "_mutation_id",
                                    "consequence._gene_id",
                                    "consequence.aa_mutation",
                                    "mutation")
                        )
                        .geneFields(
                            geneFields()
                                .includedFields(
                                    "_gene_id",
                                    "symbol")
                        )
                    )
            )
            .put(
                DocumentType.MUTATION_CENTRIC_TYPE,
                attributes()
                    .indexClassName(MutationCentricDocumentTask.class.getName())
                    .broadcastDependencies(of(BroadcastType.DONOR, BroadcastType.PROJECT, BroadcastType.GENE))
                    .fields(
                        fields()
                            .projectFields(
                                projectFields()
                                    .includedFields(
                                        "_project_id",
                                        "primary_site"
                                    )
                            )
                            .donorFields(
                                donorFields()
                                    .includedFields(
                                        "_donor_id",
                                        "disease_status_last_followup",
                                        "donor_relapse_type",
                                        "donor_sex",
                                        "donor_tumour_stage_at_diagnosis",
                                        "donor_vital_status",
                                        "_summary._age_at_diagnosis_group",
                                        "_summary._available_data_type",
                                        "_summary._state",
                                        "_summary._studies",
                                        "_summary.experimental_analysis_performed",
                                        "_project_id" // Used as a foreign key
                                    )
                            )
                            .geneFields(
                                geneFields()
                                    .includedFields(
                                        "_gene_id",
                                        "biotype",
                                        "chromosome",
                                        "curated_set",
                                        "end",
                                        "go_term.biological_process",
                                        "go_term.cellular_component",
                                        "go_term.molecular_function",
                                        "pathway",
                                        "start",
                                        "symbol",
                                        "transcripts.id",
                                        "transcripts.name"
                                    )

                            )
                            .observationFields(
                                observationFields()
                                    .includedFields(
                                        "_donor_id",
                                        "_mutation_id", // Don't index
                                        "consequence._gene_id", // Don't index
                                        "consequence._transcript_id", // Don't index
                                        "consequence.consequence_type",
                                        "consequence.aa_mutation",
                                        "consequence.gene_affected",
                                        "consequence.functional_impact_prediction_summary",
                                        "observation.platform",
                                        "observation.sequencing_strategy",
                                        "observation.verification_status"
                                    )
                            )
                            .mutationFields(
                                mutationFields()
                                    .excludedFields(
                                        "_id"
                                    )
                            )
                    )
            )
            .build();

    checkState(documentTypes.size() == DocumentType.values().length);

    return documentTypes;
  }

  private static DocumentTypeAttributes attributes() {
    return new DocumentTypeAttributes();
  }

  private static DocumentFields.Builder fields() {
    return DocumentFields.documentFields();
  }

  private static CollectionFields.Builder geneFields() {
    return CollectionFields.collectionFields();
  }

  private static CollectionFields.Builder projectFields() {
    return CollectionFields.collectionFields();
  }

  private static CollectionFields.Builder donorFields() {
    return CollectionFields.collectionFields();
  }

  private static CollectionFields.Builder observationFields() {
    return CollectionFields.collectionFields();
  }

  private static CollectionFields.Builder mutationFields() {
    return CollectionFields.collectionFields();
  }

  private static CollectionFields.Builder geneSetFields() {
    return CollectionFields.collectionFields();
  }

}
