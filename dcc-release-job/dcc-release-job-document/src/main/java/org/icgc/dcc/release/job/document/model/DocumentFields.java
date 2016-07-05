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

import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.release.job.document.model.CollectionFields.DEFAULT_COLLECTION_FIELDS;

import org.icgc.dcc.common.core.model.ReleaseCollection;
import org.icgc.dcc.release.job.document.core.DocumentTransform;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * Metadata container for describing which collection fields to include and exclude as inputs to
 * {@link DocumentTransform} s.
 */
@RequiredArgsConstructor(access = PRIVATE)
@Getter
public class DocumentFields {

  public static final DocumentFields DEFAULT_DOCUMENT_FIELDS = documentFields().build();

  @NonNull
  private final CollectionFields releaseFields;
  @NonNull
  private final CollectionFields projectFields;
  @NonNull
  private final CollectionFields donorFields;
  @NonNull
  private final CollectionFields geneFields;
  @NonNull
  private final CollectionFields observationFields;
  @NonNull
  private final CollectionFields mutationFields;
  @NonNull
  private final CollectionFields geneSetFields;
  @NonNull
  private final CollectionFields drugFields;
  @NonNull
  private final CollectionFields diagramFields;

  public CollectionFields getFields(ReleaseCollection collection) {
    switch (collection) {
    case RELEASE_COLLECTION:
      return releaseFields;
    case PROJECT_COLLECTION:
      return projectFields;
    case DONOR_COLLECTION:
      return donorFields;
    case GENE_COLLECTION:
      return geneFields;
    case OBSERVATION_COLLECTION:
      return observationFields;
    case MUTATION_COLLECTION:
      return mutationFields;
    case GENE_SET_COLLECTION:
      return geneSetFields;
    case DRUG_COLLECTION:
      return drugFields;
    case DIAGRAM_COLLECTION:
      return diagramFields;
    default:
      throw new IllegalArgumentException("Unexpected release collection: " + collection);
    }
  }

  public static Builder documentFields() {
    return new Builder();
  }

  public static class Builder {

    private CollectionFields releaseFields = DEFAULT_COLLECTION_FIELDS;
    private CollectionFields projectFields = DEFAULT_COLLECTION_FIELDS;
    private CollectionFields donorFields = DEFAULT_COLLECTION_FIELDS;
    private CollectionFields geneFields = DEFAULT_COLLECTION_FIELDS;
    private CollectionFields observationFields = DEFAULT_COLLECTION_FIELDS;
    private CollectionFields mutationFields = DEFAULT_COLLECTION_FIELDS;
    private CollectionFields geneSetFields = DEFAULT_COLLECTION_FIELDS;
    private CollectionFields drugFields = DEFAULT_COLLECTION_FIELDS;
    private CollectionFields diagramFields = DEFAULT_COLLECTION_FIELDS;

    private Builder() {
    }

    public Builder releaseFields(@NonNull CollectionFields collectionFields) {
      this.releaseFields = collectionFields;
      return this;
    }

    public Builder releaseFields(@NonNull CollectionFields.Builder builder) {
      this.releaseFields = builder.build();
      return this;
    }

    public Builder projectFields(@NonNull CollectionFields collectionFields) {
      this.projectFields = collectionFields;
      return this;
    }

    public Builder projectFields(@NonNull CollectionFields.Builder builder) {
      this.projectFields = builder.build();
      return this;
    }

    public Builder donorFields(@NonNull CollectionFields collectionFields) {
      this.donorFields = collectionFields;
      return this;
    }

    public Builder donorFields(@NonNull CollectionFields.Builder builder) {
      this.donorFields = builder.build();
      return this;
    }

    public Builder geneFields(@NonNull CollectionFields collectionFields) {
      this.geneFields = collectionFields;
      return this;
    }

    public Builder geneFields(@NonNull CollectionFields.Builder builder) {
      this.geneFields = builder.build();
      return this;
    }

    public Builder observationFields(@NonNull CollectionFields collectionFields) {
      this.observationFields = collectionFields;
      return this;
    }

    public Builder observationFields(@NonNull CollectionFields.Builder builder) {
      this.observationFields = builder.build();
      return this;
    }

    public Builder mutationFields(@NonNull CollectionFields collectionFields) {
      this.mutationFields = collectionFields;
      return this;
    }

    public Builder mutationFields(@NonNull CollectionFields.Builder builder) {
      this.mutationFields = builder.build();
      return this;
    }

    public Builder geneSetFields(@NonNull CollectionFields.Builder builder) {
      this.geneSetFields = builder.build();
      return this;
    }

    public Builder drugFields(@NonNull CollectionFields.Builder builder) {
      this.drugFields = builder.build();
      return this;
    }

    public Builder diagramFields(@NonNull CollectionFields.Builder builder) {
      this.drugFields = builder.build();
      return this;
    }

    public DocumentFields build() {
      return new DocumentFields(
          releaseFields,
          projectFields,
          donorFields,
          geneFields,
          observationFields,
          mutationFields,
          geneSetFields,
          drugFields,
          diagramFields);
    }

  }

}
