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
package org.icgc.dcc.etl2.job.annotate.model;

import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.model.FieldNames.NormalizerFieldNames.NORMALIZER_OBSERVATION_ID;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_CHROMOSOME;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_CHROMOSOME_END;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_CHROMOSOME_START;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_MUTATION_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_REFERENCE_GENOME_ALLELE;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_VARIANT_TYPE;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.core.model.FieldNames;
import org.icgc.dcc.common.core.model.FileTypes.FileType;
import org.icgc.dcc.common.core.model.Identifiable;

@Getter
@RequiredArgsConstructor(access = PRIVATE)
public enum AnnotatedFileType implements Identifiable {

  SGV(FileType.SGV_P_TYPE,
      FileType.SGV_S_TYPE,
      SUBMISSION_OBSERVATION_CHROMOSOME,
      SUBMISSION_OBSERVATION_CHROMOSOME_START,
      SUBMISSION_OBSERVATION_CHROMOSOME_END,
      SUBMISSION_OBSERVATION_VARIANT_TYPE,
      SUBMISSION_OBSERVATION_REFERENCE_GENOME_ALLELE,
      NORMALIZER_OBSERVATION_ID,
      new String[] {
          FieldNames.MUTATION_CONSEQUENCE_TYPES,
          FieldNames.AnnotatorFieldNames.ANNOTATOR_AMINO_ACID_CHANGE,
          FieldNames.AnnotatorFieldNames.ANNOTATOR_CDS_CHANGE,
          FieldNames.AnnotatorFieldNames.ANNOTATOR_PROTEIN_DOMAIN_AFFECTED,
          FieldNames.SubmissionFieldNames.SUBMISSION_GENE_AFFECTED,
          FieldNames.SubmissionFieldNames.SUBMISSION_TRANSCRIPT_AFFECTED,
          FieldNames.AnnotatorFieldNames.ANNOTATOR_GENE_BUILD_VERSION,
          FieldNames.AnnotatorFieldNames.ANNOTATOR_NOTE,
          FieldNames.NormalizerFieldNames.NORMALIZER_OBSERVATION_ID }),

  SSM(FileType.SSM_P_TYPE,
      FileType.SSM_S_TYPE,
      SUBMISSION_OBSERVATION_CHROMOSOME,
      SUBMISSION_OBSERVATION_CHROMOSOME_START,
      SUBMISSION_OBSERVATION_CHROMOSOME_END,
      SUBMISSION_OBSERVATION_MUTATION_TYPE,
      SUBMISSION_OBSERVATION_REFERENCE_GENOME_ALLELE,
      NORMALIZER_OBSERVATION_ID,
      new String[] {
          FieldNames.MUTATION_CONSEQUENCE_TYPES,
          FieldNames.CONSEQUENCE_AA_MUTATION,
          FieldNames.AnnotatorFieldNames.ANNOTATOR_CDS_MUTATION,
          FieldNames.AnnotatorFieldNames.ANNOTATOR_PROTEIN_DOMAIN_AFFECTED,
          FieldNames.SubmissionFieldNames.SUBMISSION_GENE_AFFECTED,
          FieldNames.SubmissionFieldNames.SUBMISSION_TRANSCRIPT_AFFECTED,
          FieldNames.AnnotatorFieldNames.ANNOTATOR_GENE_BUILD_VERSION,
          FieldNames.AnnotatorFieldNames.ANNOTATOR_NOTE,
          FieldNames.NormalizerFieldNames.NORMALIZER_OBSERVATION_ID });

  @NonNull
  private final FileType primaryFileType;
  @NonNull
  private final FileType secondaryFileType;

  @NonNull
  private final String chromosomeFieldName;
  @NonNull
  private final String chromosomeStartFieldName;
  @NonNull
  private final String chromosomeEndFieldName;
  @NonNull
  private final String mutationTypeFieldName;
  @NonNull
  private final String referenceAlleleFieldName;
  @NonNull
  private final String observationIdFieldName;

  @NonNull
  private final String[] secondaryFileFields;

  public static AnnotatedFileType byName(@NonNull String name) {
    for (val value : values()) {
      if (name.equals(value.getId())) {
        return value;
      }
    }

    throw new IllegalArgumentException(String.format("No '%s' with name '%s' found", AnnotatedFileType.class.getName(),
        name));
  }

  public static AnnotatedFileType byPath(@NonNull Path path) {
    val inputFileName = path.getName();
    for (val value : values()) {
      if (inputFileName.equals(value.getInputFileName())) {
        return value;
      }
    }

    throw new IllegalArgumentException(String.format("No '%s' with inputFileName '%s' found",
        AnnotatedFileType.class.getName(),
        inputFileName));
  }

  @Override
  public String getId() {
    return name().toLowerCase();
  }

  public String getInputFileName() {
    return primaryFileType.getHarmonizedOutputFileName();
  }

  public String getOutputFileName() {
    return secondaryFileType.getHarmonizedOutputFileName();
  }

  public int getFieldsNumber() {
    return secondaryFileFields.length;
  }

}
