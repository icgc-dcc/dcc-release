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

import static org.icgc.dcc.common.core.model.Entity.DONOR;
import static org.icgc.dcc.common.core.model.Entity.GENE;
import static org.icgc.dcc.common.core.model.Entity.GENE_SET;
import static org.icgc.dcc.common.core.model.Entity.MUTATION;
import static org.icgc.dcc.common.core.model.Entity.OBSERVATION;
import static org.icgc.dcc.common.core.model.Entity.PATHWAY;
import static org.icgc.dcc.common.core.model.Entity.PROJECT;
import static org.icgc.dcc.common.core.model.Entity.RELEASE;
import static org.icgc.dcc.common.core.model.ReleaseCollection.DIAGRAM_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.DONOR_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.GENE_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.GENE_SET_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.MUTATION_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.OBSERVATION_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.PROJECT_COLLECTION;
import static org.icgc.dcc.common.core.model.ReleaseCollection.RELEASE_COLLECTION;
import static org.icgc.dcc.release.core.document.DocumentClassifier.CENTRIC;
import lombok.Getter;
import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.common.core.model.Entity;
import org.icgc.dcc.common.core.model.IndexType;
import org.icgc.dcc.common.core.model.ReleaseCollection;
import org.icgc.dcc.release.core.job.FileType;

import com.google.common.collect.ImmutableList;

@Getter
public enum BaseDocumentType {

  /**
   * Diagram type(s).
   */
  DIAGRAM_TYPE(
      attributes()
          .name("diagram")
          .entity(PATHWAY)
          .collection(DIAGRAM_COLLECTION)
          .outputFileType(FileType.DIAGRAM_DOCUMENT)
  ),

  /**
   * Release type(s).
   */
  RELEASE_TYPE(
      attributes()
          .name("release")
          .entity(RELEASE)
          .collection(RELEASE_COLLECTION)
          .outputFileType(FileType.RELEASE_DOCUMENT)
  ),

  /**
   * Gene Set type(s).
   */
  GENE_SET_TYPE(attributes()
      .name("gene-set")
      .entity(GENE_SET)
      .collection(GENE_SET_COLLECTION)
      .outputFileType(FileType.GENE_SET_DOCUMENT)
  ),

  GENE_SET_TEXT_TYPE(attributes()
      .name("gene-set-text")
      .entity(GENE_SET)
      .collection(GENE_SET_COLLECTION)
      .outputFileType(FileType.GENE_SET_TEXT_DOCUMENT)
  ),

  /**
   * Project type(s).
   */
  PROJECT_TYPE(
      attributes()
          .name("project")
          .entity(PROJECT)
          .collection(PROJECT_COLLECTION)
          .outputFileType(FileType.PROJECT_DOCUMENT)
  ),

  PROJECT_TEXT_TYPE(
      attributes()
          .name("project-text")
          .entity(PROJECT)
          .collection(PROJECT_COLLECTION)
          .outputFileType(FileType.PROJECT_TEXT_DOCUMENT)
  ),

  /**
   * Donor type(s).
   */
  DONOR_TYPE(
      attributes()
          .name("donor")
          .entity(DONOR)
          .collection(DONOR_COLLECTION)
          .outputFileType(FileType.DONOR_DOCUMENT)
  ),

  DONOR_TEXT_TYPE(
      attributes()
          .name("donor-text")
          .entity(DONOR)
          .collection(DONOR_COLLECTION)
          .outputFileType(FileType.DONOR_TEXT_DOCUMENT)
  ),

  DONOR_CENTRIC_TYPE(
      attributes()
          .name("donor-centric")
          .entity(DONOR)
          .collection(DONOR_COLLECTION)
          .classifier(CENTRIC)
          .outputFileType(FileType.DONOR_CENTRIC_DOCUMENT)
  ),

  /**
   * Gene type(s).
   */
  GENE_TYPE(
      attributes()
          .name("gene")
          .entity(GENE)
          .collection(GENE_COLLECTION)
          .outputFileType(FileType.GENE_DOCUMENT)
  ),

  GENE_TEXT_TYPE(
      attributes()
          .name("gene-text")
          .entity(GENE)
          .collection(GENE_COLLECTION)
          .outputFileType(FileType.GENE_TEXT_DOCUMENT)
  ),

  GENE_CENTRIC_TYPE(
      attributes()
          .name("gene-centric")
          .entity(GENE)
          .collection(GENE_COLLECTION)
          .classifier(CENTRIC)
          .outputFileType(FileType.GENE_CENTRIC_DOCUMENT)
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
          .outputFileType(FileType.OBSERVATION_CENTRIC_DOCUMENT)
  ),

  /**
   * Mutation type(s).
   */
  MUTATION_TEXT_TYPE(
      attributes()
          .name("mutation-text")
          .entity(MUTATION)
          .collection(MUTATION_COLLECTION)
          .outputFileType(FileType.MUTATION_TEXT_DOCUMENT)
  ),

  MUTATION_CENTRIC_TYPE(
      attributes()
          .name("mutation-centric")
          .entity(MUTATION)
          .collection(MUTATION_COLLECTION)
          .classifier(CENTRIC)
          .outputFileType(FileType.MUTATION_CENTRIC_DOCUMENT)
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
   * The source collection.
   */
  private final ReleaseCollection collection;

  private BaseDocumentType(@NonNull DocumentTypeAttributes attributes) {
    this.entity = attributes.entity;
    this.name = attributes.name;
    this.classifier = attributes.classifier;
    this.outputFileType = attributes.outputFileType;
    this.collection = attributes.collection;
  }

  public static Iterable<BaseDocumentType> convert(Iterable<IndexType> indexTypes) {
    val types = ImmutableList.<BaseDocumentType> builder();
    for (val indexType : indexTypes) {
      types.add(BaseDocumentType.byName(indexType.getName()));
    }

    return types.build();
  }

  public static BaseDocumentType byName(@NonNull String name) {
    for (val value : values()) {
      if (name.equals(value.name)) {
        return value;
      }
    }

    throw new IllegalArgumentException("No '" + BaseDocumentType.class.getName() + "' value with name '" + name + "' found");
  }

  @Override
  public String toString() {
    return name;
  }

  private static DocumentTypeAttributes attributes() {
    return new DocumentTypeAttributes();
  }

}
