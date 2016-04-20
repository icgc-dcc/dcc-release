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
package org.icgc.dcc.release.core.document;

import static org.icgc.dcc.common.core.model.Entity.DIAGRAM;
import static org.icgc.dcc.common.core.model.Entity.DONOR;
import static org.icgc.dcc.common.core.model.Entity.DRUG;
import static org.icgc.dcc.common.core.model.Entity.GENE;
import static org.icgc.dcc.common.core.model.Entity.GENE_SET;
import static org.icgc.dcc.common.core.model.Entity.MUTATION;
import static org.icgc.dcc.common.core.model.Entity.OBSERVATION;
import static org.icgc.dcc.common.core.model.Entity.PROJECT;
import static org.icgc.dcc.common.core.model.Entity.RELEASE;
import static org.icgc.dcc.common.core.model.FieldNames.DIAGRAM_ID;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_ID;
import static org.icgc.dcc.common.core.model.FieldNames.DRUG_ID;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_SET_ID;
import static org.icgc.dcc.common.core.model.FieldNames.PROJECT_ID;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_ID;
import static org.icgc.dcc.release.core.document.DocumentClassifier.CENTRIC;
import lombok.Getter;
import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.common.core.model.Entity;
import org.icgc.dcc.common.core.model.FieldNames;
import org.icgc.dcc.common.core.model.IndexType;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.util.FieldNames.IndexFieldNames;

import com.google.common.collect.ImmutableList;

@Getter
public enum DocumentType {

  /**
   * Diagram type(s).
   */
  DIAGRAM_TYPE(
      attributes()
          .name("diagram")
          .entity(DIAGRAM)
          .outputFileType(FileType.DIAGRAM_DOCUMENT)
          .primaryKey(DIAGRAM_ID)
  ),

  /**
   * Drug type(s).
   */
  DRUG_TEXT_TYPE(
      attributes()
          .name("drug-text")
          .entity(DRUG)
          .outputFileType(FileType.DRUG_TEXT_DOCUMENT)
          .primaryKey(IndexFieldNames.TEXT_TYPE_ID)
  ),

  DRUG_CENTRIC_TYPE(
      attributes()
          .name("drug-centric")
          .entity(DRUG)
          .outputFileType(FileType.DRUG_CENTRIC_DOCUMENT)
          .primaryKey(DRUG_ID)
  ),

  /**
   * Release type(s).
   */
  RELEASE_TYPE(
      attributes()
          .name("release")
          .entity(RELEASE)
          .outputFileType(FileType.RELEASE_DOCUMENT)
          .primaryKey(RELEASE_ID)
  ),

  /**
   * Gene Set type(s).
   */
  GENE_SET_TYPE(attributes()
      .name("gene-set")
      .entity(GENE_SET)
      .outputFileType(FileType.GENE_SET_DOCUMENT)
      .primaryKey(GENE_SET_ID)
  ),

  GENE_SET_TEXT_TYPE(attributes()
      .name("gene-set-text")
      .entity(GENE_SET)
      .outputFileType(FileType.GENE_SET_TEXT_DOCUMENT)
      .primaryKey(IndexFieldNames.TEXT_TYPE_ID)
  ),

  /**
   * Project type(s).
   */
  PROJECT_TYPE(
      attributes()
          .name("project")
          .entity(PROJECT)
          .outputFileType(FileType.PROJECT_DOCUMENT)
          .primaryKey(PROJECT_ID)
  ),

  PROJECT_TEXT_TYPE(
      attributes()
          .name("project-text")
          .entity(PROJECT)
          .outputFileType(FileType.PROJECT_TEXT_DOCUMENT)
          .primaryKey(IndexFieldNames.TEXT_TYPE_ID)
  ),

  /**
   * Donor type(s).
   */
  DONOR_TYPE(
      attributes()
          .name("donor")
          .entity(DONOR)
          .outputFileType(FileType.DONOR_DOCUMENT)
          .primaryKey(DONOR_ID)
  ),

  DONOR_TEXT_TYPE(
      attributes()
          .name("donor-text")
          .entity(DONOR)
          .outputFileType(FileType.DONOR_TEXT_DOCUMENT)
          .primaryKey(IndexFieldNames.TEXT_TYPE_ID)
  ),

  DONOR_CENTRIC_TYPE(
      attributes()
          .name("donor-centric")
          .entity(DONOR)
          .classifier(CENTRIC)
          .outputFileType(FileType.DONOR_CENTRIC_DOCUMENT)
          .primaryKey(DONOR_ID)
  ),

  /**
   * Gene type(s).
   */
  GENE_TYPE(
      attributes()
          .name("gene")
          .entity(GENE)
          .outputFileType(FileType.GENE_DOCUMENT)
          .primaryKey(GENE_ID)
  ),

  GENE_TEXT_TYPE(
      attributes()
          .name("gene-text")
          .entity(GENE)
          .outputFileType(FileType.GENE_TEXT_DOCUMENT)
          .primaryKey(IndexFieldNames.TEXT_TYPE_ID)
  ),

  GENE_CENTRIC_TYPE(
      attributes()
          .name("gene-centric")
          .entity(GENE)
          .classifier(CENTRIC)
          .outputFileType(FileType.GENE_CENTRIC_DOCUMENT)
          .primaryKey(GENE_ID)
  ),

  /**
   * Observation type(s).
   */
  OBSERVATION_CENTRIC_TYPE(
      attributes()
          .name("observation-centric")
          .entity(OBSERVATION)
          .classifier(CENTRIC)
          .outputFileType(FileType.OBSERVATION_CENTRIC_DOCUMENT)
          .primaryKey("")

  ),

  /**
   * Mutation type(s).
   */
  MUTATION_TEXT_TYPE(
      attributes()
          .name("mutation-text")
          .entity(MUTATION)
          .outputFileType(FileType.MUTATION_TEXT_DOCUMENT)
          .primaryKey(IndexFieldNames.TEXT_TYPE_ID)
  ),

  MUTATION_CENTRIC_TYPE(
      attributes()
          .name("mutation-centric")
          .entity(MUTATION)
          .classifier(CENTRIC)
          .outputFileType(FileType.MUTATION_CENTRIC_DOCUMENT)
          .primaryKey(FieldNames.MUTATION_ID)
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
   * Output file type of the document.
   */
  private final FileType outputFileType;

  /**
   * Name of the primary key of the document.
   */
  private final String primaryKey;

  private DocumentType(@NonNull DocumentTypeAttributes attributes) {
    this.entity = attributes.entity;
    this.name = attributes.name;
    this.classifier = attributes.classifier;
    this.outputFileType = attributes.outputFileType;
    this.primaryKey = attributes.primaryKey;
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

    throw new IllegalArgumentException("No '" + DocumentType.class.getName() + "' value with name '" + name
        + "' found");
  }

  @Override
  public String toString() {
    return name;
  }

  private static DocumentTypeAttributes attributes() {
    return new DocumentTypeAttributes();
  }

}
