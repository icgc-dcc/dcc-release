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
import org.icgc.dcc.release.job.document.task.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

@NoArgsConstructor(access = PRIVATE)
public final class DocumentTypes {

  private static final Map<DocumentType, DocumentTypeAttributes> DOCUMENT_TYPES = defineDocumentTypes();

  public static String getDocumentClassName(@NonNull DocumentType documentType) {
    return DOCUMENT_TYPES.get(documentType).documentClassName();
  }

  public static Collection<BroadcastType> getBroadcastDependencies(@NonNull DocumentType documentType) {
    return DOCUMENT_TYPES.get(documentType).broadcastDependencies();
  }

  public static DocumentFields getFields(@NonNull DocumentType documentType) {
    return DOCUMENT_TYPES.get(documentType).fields();
  }

  private static Map<DocumentType, DocumentTypeAttributes> defineDocumentTypes() {
    val documentTypes = ImmutableMap.<DocumentType, DocumentTypeAttributes> builder()
        .put(DocumentType.DIAGRAM_TYPE, defineDiagramType())
        .put(DocumentType.DRUG_CENTRIC_TYPE, defineDrugCentricType())
        .put(DocumentType.RELEASE_TYPE, defineReleaseType())
        .put(DocumentType.DRUG_TEXT_TYPE, defineDrugTextType())
        .put(DocumentType.GENE_SET_TYPE, defineGeneSetType())
        .put(DocumentType.GENE_SET_TEXT_TYPE, defineGeneSetTextType())
        .put(DocumentType.PROJECT_TYPE, defineProjectType())
        .put(DocumentType.PROJECT_TEXT_TYPE, defineProjectTextType())
        .put(DocumentType.DONOR_TYPE, defineDonorType())
        .put(DocumentType.DONOR_TEXT_TYPE, defineDonorTextType())
        .put(DocumentType.DONOR_CENTRIC_TYPE, defineDonorCentricType())
        .put(DocumentType.GENE_TYPE, defineGeneType())
        .put(DocumentType.GENE_TEXT_TYPE, defineGeneTextType())
        .put(DocumentType.GENE_CENTRIC_TYPE, defineGeneCentricType())
        .put(DocumentType.OBSERVATION_CENTRIC_TYPE, defineObservationCentricType())
        .put(DocumentType.MUTATION_TEXT_TYPE, defineMutationTextType())
        .put(DocumentType.MUTATION_CENTRIC_TYPE, defineMutationCentricType())
        .build();

    checkState(documentTypes.size() == DocumentType.values().length);

    return documentTypes;
  }

  private static DocumentTypeAttributes defineDiagramType() {
    return attributes().documentClassName(DiagramDocumentTask.class.getName());
  }

  private static DocumentTypeAttributes defineDrugCentricType() {
    return attributes().documentClassName(DrugCentricDocumentTask.class.getName());
  }

  private static DocumentTypeAttributes defineReleaseType() {
    return attributes().documentClassName(ReleaseDocumentTask.class.getName());
  }

  private static DocumentTypeAttributes defineDrugTextType() {
    return attributes()
        .documentClassName(DrugTextDocumentTask.class.getName())
        .fields(fields()
            .drugFields(
                drugFields()
                    .excludedFields(
                        "_id",
                        "cancer_trial_count",
                        "genes",
                        "large_image_url",
                        "small_image_url"
                    )
            )
        );
  }

  private static DocumentTypeAttributes defineGeneSetType() {
    return attributes()
        .documentClassName(GeneSetDocumentTask.class.getName())
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
        );
  }

  private static DocumentTypeAttributes defineGeneSetTextType() {
    return attributes()
        .documentClassName(GeneSetTextDocumentTask.class.getName())
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
        );
  }

  private static DocumentTypeAttributes defineProjectType() {
    return attributes().documentClassName(ProjectDocumentTask.class.getName());
  }

  private static DocumentTypeAttributes defineProjectTextType() {
    return attributes()
        .documentClassName(ProjectTextDocumentTask.class.getName())
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
        );
  }

  private static DocumentTypeAttributes defineDonorType() {
    return attributes()
        .documentClassName(DonorDocumentTask.class.getName())
        .broadcastDependencies(ImmutableList.of(BroadcastType.PROJECT))
        .fields(fields()
            .donorFields(
                donorFields()
                    .excludedFields(
                        "_id",
                        "gene")
            )
        );
  }

  private static DocumentTypeAttributes defineDonorTextType() {
    return attributes()
        .documentClassName(DonorTextDocumentTask.class.getName())
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
        );
  }

  private static DocumentTypeAttributes defineDonorCentricType() {
    return attributes()
        .documentClassName(DonorCentricDocumentTask.class.getName())
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
                        "gene",
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
                        "observation.verification_status",
                        "observation._study"
                    )
            )
        );
  }

  private static DocumentTypeAttributes defineGeneType() {
    return attributes()
        .documentClassName(GeneDocumentTask.class.getName())
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
        );
  }

  private static DocumentTypeAttributes defineGeneTextType() {
    return attributes()
        .documentClassName(GeneTextDocumentTask.class.getName())
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
        );
  }

  private static DocumentTypeAttributes defineGeneCentricType() {
    return attributes()
        .documentClassName(GeneCentricDocumentTask.class.getName())
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
                            "observation.verification_status",
                            "observation._study"
                        )
                )
                .geneFields(
                    geneFields()
                        .includedFields(
                            "_gene_id",
                            "biotype",
                            "chromosome",
                            "description",
                            "donor",
                            "end",
                            "external_db_ids",
                            "name",
                            "sets",
                            "start",
                            "strand",
                            "symbol",
                            "transcripts.biotype",
                            "transcripts.cdna_coding_end",
                            "transcripts.cdna_coding_start",
                            "transcripts.coding_region_end",
                            "transcripts.coding_region_start",
                            "transcripts.exons.cdna_coding_end",
                            "transcripts.exons.cdna_coding_start",
                            "transcripts.exons.cdna_end",
                            "transcripts.exons.cdna_start",
                            "transcripts.exons.genomic_coding_end",
                            "transcripts.exons.genomic_coding_start",
                            "transcripts.exons.end",
                            "transcripts.exons.start",
                            "transcripts.domains",
                            "transcripts.id",
                            "transcripts.length_amino_acid",
                            "transcripts.name"
                        )
                )
        );
  }

  private static DocumentTypeAttributes defineObservationCentricType() {
    return attributes()
        .documentClassName(ObservationCentricDocumentTask.class.getName())
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
                            "chromosome",
                            "start",
                            "end",
                            "sets")

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
                            "_project_id",

                            // Consequence
                            "consequence.aa_change",
                            "consequence.cds_change",
                            "consequence.note",
                            "consequence.protein_domain_affected",
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
        );
  }

  private static DocumentTypeAttributes defineMutationTextType() {
    return attributes()
        .documentClassName(MutationTextDocumentTask.class.getName())
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
        );
  }

  private static DocumentTypeAttributes defineMutationCentricType() {
    return attributes()
        .documentClassName(MutationCentricDocumentTask.class.getName())
        .broadcastDependencies(of(BroadcastType.DONOR, BroadcastType.PROJECT, BroadcastType.GENE, BroadcastType.CLINVAR, BroadcastType.CIVIC))
        .fields(
            fields()
                .projectFields(
                    projectFields()
                        .includedFields(
                            "_project_id",
                            "_summary._affected_donor_count",
                            "_summary._ssm_tested_donor_count",
                            "primary_site",
                            "project_name",
                            "tumour_type",
                            "tumour_subtype"
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
                            "end",
                            "external_db_ids.uniprotkb_swissprot",
                            "sets",
                            "start",
                            "strand",
                            "symbol",
                            "transcripts.biotype",
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
                            "consequence.cds_mutation",
                            "consequence.gene_affected",
                            "consequence.functional_impact_prediction_summary",
                            "observation.platform",
                            "observation.sequencing_strategy",
                            "observation.verification_status",
                            "observation._study"
                        )
                )
                .mutationFields(
                    mutationFields()
                        .excludedFields(
                            "_id"
                        )
                )
        );
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

  private static CollectionFields.Builder drugFields() {
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
