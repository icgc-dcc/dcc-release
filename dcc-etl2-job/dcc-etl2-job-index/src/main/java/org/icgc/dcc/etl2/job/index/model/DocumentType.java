/*
 * Copyright (c) 2013 The Ontario Institute for Cancer Research. All rights reserved.
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
package org.icgc.dcc.etl2.job.index.model;

import static org.icgc.dcc.common.core.model.Entity.DONOR;
import static org.icgc.dcc.common.core.model.Entity.GENE;
import static org.icgc.dcc.common.core.model.Entity.GENE_SET;
import static org.icgc.dcc.common.core.model.Entity.MUTATION;
import static org.icgc.dcc.common.core.model.Entity.OBSERVATION;
import static org.icgc.dcc.common.core.model.Entity.PROJECT;
import static org.icgc.dcc.common.core.model.Entity.RELEASE;
import static org.icgc.dcc.common.core.model.ReleaseCollection.DONOR_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.GENE_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.GENE_SET_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.MUTATION_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.OBSERVATION_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.PROJECT_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.RELEASE_COLLECTION;
import static org.icgc.dcc.etl2.job.index.model.DocumentClassifier.CENTRIC;
import lombok.Getter;
import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.common.core.model.Entity;
import org.icgc.dcc.common.core.model.IndexType;
import org.icgc.dcc.common.core.model.ReleaseCollection;
import org.icgc.dcc.etl2.job.index.core.Document;
import org.icgc.dcc.etl2.job.index.core.DocumentContext;
import org.icgc.dcc.etl2.job.index.core.DocumentTransform;
import org.icgc.dcc.etl2.job.index.transform.DonorCentricDocumentTransform;
import org.icgc.dcc.etl2.job.index.transform.DonorDocumentTransform;
import org.icgc.dcc.etl2.job.index.transform.DonorTextDocumentTransform;
import org.icgc.dcc.etl2.job.index.transform.GeneCentricDocumentTransform;
import org.icgc.dcc.etl2.job.index.transform.GeneSetTextDocumentTransform;
import org.icgc.dcc.etl2.job.index.transform.GeneTextDocumentTransform;
import org.icgc.dcc.etl2.job.index.transform.MutationCentricDocumentTransform;
import org.icgc.dcc.etl2.job.index.transform.MutationTextDocumentTransform;
import org.icgc.dcc.etl2.job.index.transform.ObservationCentricDocumentTransform;
import org.icgc.dcc.etl2.job.index.transform.ProjectTextDocumentTransform;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

/**
 * Metadata for document types.
 * <p/>
 * Eventually this entire class can be externalized and data driven.
 */
@Getter
public enum DocumentType {

  /**
   * Release type(s).
   */
  RELEASE_TYPE(
      attributes()
          .name("release")
          .entity(RELEASE)
          .collection(RELEASE_COLLECTION)
          .batchSize(1000)
          .statusInterval(1)
  ),

  /**
   * Gene Set type(s).
   */
  GENE_SET_TYPE(attributes()
      .name("gene-set")
      .entity(GENE_SET)
      .collection(GENE_SET_COLLECTION)
      // .transform(new GeneSetDocumentTransform())
      .transform(new DocumentTransform() {

        @Override
        public Document transformDocument(ObjectNode root, DocumentContext context) {
          return null;

        }
      })
      .statusInterval(1000)
      .batchSize(1000)
      .fields(
          fields()
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
      )),
  GENE_SET_TEXT_TYPE(attributes()
      .name("gene-set-text")
      .entity(GENE_SET)
      .collection(GENE_SET_COLLECTION)
      .transform(new GeneSetTextDocumentTransform())
      .statusInterval(1000)
      .batchSize(1000)
      .fields(
          fields()
              .geneSetFields(
                  geneSetFields()
                      .includedFields(
                          "id",
                          "name",
                          "type",
                          "source",
                          "go_term.alt_ids")
              )
      )),

  /**
   * Project type(s).
   */
  PROJECT_TYPE(
      attributes()
          .name("project")
          .entity(PROJECT)
          .collection(PROJECT_COLLECTION)
          .batchSize(100)
          .statusInterval(10)
  ),
  PROJECT_TEXT_TYPE(
      attributes()
          .name("project-text")
          .entity(PROJECT)
          .collection(PROJECT_COLLECTION)
          .transform(new ProjectTextDocumentTransform())
          .batchSize(100)
          .statusInterval(10)
          .fields(
              fields()
                  .projectFields(
                      projectFields()
                          .includedFields(
                              "_project_id",
                              "project_name",
                              "tumour_type",
                              "tumour_subtype",
                              "primary_site")
                  )
          )
  ),

  /**
   * Donor type(s).
   */
  DONOR_TYPE(
      attributes()
          .name("donor")
          .entity(DONOR)
          .collection(DONOR_COLLECTION)
          .transform(new DonorDocumentTransform())
          .batchSize(10000)
          .statusInterval(1000)
          .fields(
              fields()
                  .donorFields(
                      donorFields()
                          .excludedFields(
                              "_id",
                              "gene")
                  )
          )
  ),
  DONOR_TEXT_TYPE(
      attributes()
          .name("donor-text")
          .entity(DONOR)
          .collection(DONOR_COLLECTION)
          .transform(new DonorTextDocumentTransform())
          .batchSize(10000)
          .statusInterval(1000)
          .fields(
              fields()
                  .donorFields(
                      donorFields()
                          .includedFields(
                              "_donor_id",
                              "_project_id",
                              "donor_id",
                              "specimen._specimen_id",
                              "specimen.specimen_id",
                              "specimen.sample._sample_id",
                              "specimen.sample.analyzed_sample_id")
                  )
          )
  ),
  DONOR_CENTRIC_TYPE(
      attributes()
          .name("donor-centric")
          .entity(DONOR)
          .collection(DONOR_COLLECTION)
          .classifier(CENTRIC)
          .transform(new DonorCentricDocumentTransform())
          .statusInterval(1000)
          .batchSize(1)
          .fields(
              fields()
                  .projectFields(
                      projectFields()
                          .includedFields(
                              // Primary key
                              "_project_id",

                              // Data
                              "primary_site",
                              "project_name")
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
                              "gene._summary._ssm_count",
                              "_summary",

                              // Data
                              "donor_id",
                              "disease_status_last_followup",
                              "donor_age_at_diagnosis",
                              "donor_age_at_enrollment",
                              "donor_age_at_last_followup",
                              "donor_diagnosis_icd10",
                              "donor_interval_of_last_followup",
                              "donor_relapse_interval",
                              "donor_relapse_type",
                              "donor_sex",
                              "donor_survival_time",
                              "donor_tumour_stage_at_diagnosis",
                              "donor_tumour_stage_at_diagnosis_supplemental",
                              "donor_tumour_staging_system_at_diagnosis",
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
                              "consequence._gene_id",

                              // Data
                              "consequence.consequence_type",
                              "consequence.functional_impact_prediction_summary",
                              "_type",
                              "mutation_type",
                              "platform",
                              "validation_status",
                              "verification_status",
                              "chromosome",
                              "chromosome_end",
                              "chromosome_start",
                              "observation")
                  )
          )
  ),

  /**
   * Gene type(s).
   */
  GENE_TYPE(
      attributes()
          .name("gene")
          .entity(GENE)
          .collection(GENE_COLLECTION)
          .batchSize(1000)
          .statusInterval(1000)
          .fields(
              fields()
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
  ),
  GENE_TEXT_TYPE(
      attributes()
          .name("gene-text")
          .entity(GENE)
          .collection(GENE_COLLECTION)
          .transform(new GeneTextDocumentTransform())
          .batchSize(1000)
          .statusInterval(1000)
          .fields(
              fields()
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
  ),
  GENE_CENTRIC_TYPE(
      attributes()
          .name("gene-centric")
          .entity(GENE)
          .collection(GENE_COLLECTION)
          .classifier(CENTRIC)
          .transform(new GeneCentricDocumentTransform())
          .batchSize(1000)
          .statusInterval(1000)
          .fields(
              fields()
                  .donorFields(
                      donorFields()
                          .excludedFields(
                              "_id",
                              "gene",
                              "specimen")
                  )
                  .observationFields(
                      observationFields()
                          .excludedFields(
                              "_id",
                              "functional_impact_prediction_summary",
                              "consequence.functional_impact_prediction",
                              "consequence_type")
                  )
          )
  ),

  /**
   * Observation type(s).
   */
  OBSERVATION_CENTRIC_TYPE(
      attributes()
          .name("observation-centric")
          .entity(OBSERVATION)
          .collection(OBSERVATION_COLLECTION)
          .classifier(CENTRIC)
          .transform(new ObservationCentricDocumentTransform())
          .batchSize(200)
          .statusInterval(100000)
          .fields(
              fields()
                  .projectFields(
                      projectFields()
                          .includedFields(
                              "_project_id",
                              "project_name",
                              "primary_site")
                  )
                  .donorFields(
                      donorFields()
                          .excludedFields(
                              "_id",
                              "gene",
                              "specimen")
                  )
                  .geneFields(
                      geneFields()
                          .excludedFields(
                              "_id",
                              "project",
                              "donor",
                              "transcripts")

                  )
                  .observationFields(
                      observationFields()
                          .excludedFields(
                              "_id",
                              "functional_impact_prediction_summary",
                              "consequence.functional_impact_prediction",
                              "consequence_type")
                  )
          )
  ),

  /**
   * Mutation type(s).
   */
  MUTATION_TEXT_TYPE(
      attributes()
          .name("mutation-text")
          .entity(MUTATION)
          .collection(MUTATION_COLLECTION)
          .transform(new MutationTextDocumentTransform())
          .batchSize(1000)
          .statusInterval(100000)
          .fields(
              fields()
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
  ),
  MUTATION_CENTRIC_TYPE(
      attributes()
          .name("mutation-centric")
          .entity(MUTATION)
          .collection(MUTATION_COLLECTION)
          .classifier(CENTRIC)
          .transform(new MutationCentricDocumentTransform())
          .batchSize(1000)
          .statusInterval(100000)
          .fields(
              fields()
                  .donorFields(
                      donorFields()
                          .excludedFields(
                              "_id",
                              "gene",
                              "specimen")
                  )
                  .geneFields(
                      geneFields()
                          .excludedFields(
                              "_id",
                              "project",
                              "donor",
                              "transcripts.domains",
                              "transcripts.exons")

                  )
          )
  );

  /**
   * The corresponding entity of the index type.
   */
  private final Entity entity;

  /**
   * The name of the index type.
   */
  private final String name;

  /**
   * The classifier of the index type.
   */
  private final DocumentClassifier classifier;

  /**
   * The document transform.
   */
  private final DocumentTransform transform;

  /**
   * The document status interval.
   */
  private final int statusInterval;

  /**
   * The document batch size.
   */
  private final int batchSize;

  /**
   * The source collection.
   */
  private final ReleaseCollection collection;

  /**
   * The source collection fields used to create the index.
   */
  private final DocumentFields fields;

  private DocumentType(@NonNull DocumentTypeAttributes attributes) {
    this.entity = attributes.entity;
    this.name = attributes.name;
    this.classifier = attributes.classifier;
    this.transform = attributes.transform;
    this.batchSize = attributes.batchSize;
    this.statusInterval = attributes.statusInterval;
    this.collection = attributes.collection;
    this.fields = attributes.fields;
  }

  public static Iterable<DocumentType> convert(Iterable<IndexType> indexTypes) {
    val types = ImmutableList.<DocumentType> builder();
    for (val indexType : indexTypes) {
      types.add(DocumentType.byName(indexType.getName()));
    }

    return types.build();
  }

  public static DocumentType byName(@NonNull String name) {
    for (val value : values()) {
      if (name.equals(value.name)) {
        return value;
      }
    }

    throw new IllegalArgumentException("No '" + DocumentType.class.getName() + "' value with name '" + name + "' found");
  }

  @Override
  public String toString() {
    return name;
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
