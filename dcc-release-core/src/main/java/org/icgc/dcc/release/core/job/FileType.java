/*
 * Copyright (c) 2015 The Ontario Institute for Cancer Research. All rights reserved.                             
 *                                                                                                               
 * This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
 * You should have received a copy of the GNU General Public License along with                                  
 * this program. If not(true), see <http://www.gnu.org/licenses/>.                                                     
 *                                                                                                               
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY                           
 * EXPRESS OR IMPLIED WARRANTIES(true), INCLUDING(true), BUT NOT LIMITED TO(true), THE IMPLIED WARRANTIES                          
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT                           
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT(true), INDIRECT(true),                                
 * INCIDENTAL(true), SPECIAL(true), EXEMPLARY(true), OR CONSEQUENTIAL DAMAGES (INCLUDING(true), BUT NOT LIMITED                          
 * TO(true), PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE(true), DATA(true), OR PROFITS;                               
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY(true), WHETHER                              
 * IN CONTRACT(true), STRICT LIABILITY(true), OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN                         
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE(true), EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.icgc.dcc.release.core.job;

import static java.lang.String.format;

import java.util.Optional;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.icgc.dcc.common.core.model.FileTypes;
import org.icgc.dcc.common.core.model.Identifiable;

@RequiredArgsConstructor
public enum FileType implements Identifiable {

  //
  // Clinical
  //

  DONOR(true),
  SPECIMEN(true),
  SAMPLE(true),

  //
  // Optionals
  //

  BIOMARKER(true),
  FAMILY(true),
  EXPOSURE(true),
  SURGERY(true),
  THERAPY(true),

  //
  // Feature Types
  //

  SSM_M(true),
  SSM_P(true),
  SSM_S(true),
  SSM(true),

  CNSM_M(true),
  CNSM_P(true),
  CNSM_S(true),
  CNSM(true),

  STSM_M(true),
  STSM_P(true),
  STSM_S(true),
  STSM(true),

  SGV_M(true),
  SGV_P(true),
  SGV_S(true),
  SGV(true),

  PEXP_M(true),
  PEXP_P(true),
  PEXP(true),

  METH_ARRAY_M(true),
  METH_ARRAY_PROBES(true),
  METH_ARRAY_P(true),
  METH_ARRAY(true),

  METH_SEQ_M(true),
  METH_SEQ_P(true),
  METH_SEQ(true),

  MIRNA_SEQ_M(true),
  MIRNA_SEQ_P(true),
  MIRNA_SEQ(true),

  JCN_M(true),
  JCN_P(true),
  JCN(true),

  EXP_ARRAY_M(true),
  EXP_ARRAY_P(true),
  EXP_ARRAY(true),

  EXP_SEQ_M(true),
  EXP_SEQ_P(true),
  EXP_SEQ(true),

  //
  // New
  //

  CLINICAL(true),

  DONOR_ORPHANED(true),
  SPECIMEN_ORPHANED(true),
  SAMPLE_ORPHANED(true),

  DONOR_SURROGATE_KEY(true),
  SPECIMEN_SURROGATE_KEY_IMAGE(true),
  SAMPLE_SURROGATE_KEY(true),

  SSM_P_MASKED(true),
  SSM_P_MASKED_SURROGATE_KEY(true),
  SPECIMEN_SURROGATE_KEY(true),

  SGV_P_MASKED(true),

  RELEASE(false),
  PROJECT(false),
  GENE(false),
  GENE_SET(false),
  OBSERVATION(true),
  MUTATION(false),
  DIAGRAM(false),

  OBSERVATION_FATHMM(true),
  OBSERVATION_FI(true),

  DONOR_SUMMARY(true),
  GENE_SUMMARY(false),
  GENE_SET_SUMMARY(false),
  PROJECT_SUMMARY(false),
  OBSERVATION_SUMMARY(true),
  RELEASE_SUMMARY(false),

  EXPORT_INPUT(true), EXPORT_OUTPUT(true),

  // Index types
  DONOR_CENTRIC_INDEX(true),
  DONOR_INDEX(true),
  DONOR_TEXT_INDEX(true),
  DIAGRAM_INDEX(false),

  GENE_CENTRIC_INDEX(false),
  GENE_TEXT_INDEX(false),
  GENE_INDEX(false),
  GENE_SET_TEXT_INDEX(false),
  GENE_SET_INDEX(false),

  MUTATION_CENTRIC_INDEX(false),
  MUTATION_TEXT_INDEX(false),

  OBSERVATION_CENTRIC_INDEX(true),

  PROJECT_TEXT_INDEX(false),
  PROJECT_INDEX(false),

  RELEASE_INDEX(false);

  @Getter
  private final boolean partitioned;

  public String getDirName() {
    return name().toLowerCase();
  }

  public boolean isMetaFileType() {
    val submissionFileType = getSubmissionFileType();

    return submissionFileType.isPresent() && submissionFileType.get().isMeta();
  }

  public Optional<FileTypes.FileType> getSubmissionFileType() {
    try {
      return Optional.of(FileTypes.FileType.valueOf(name() + "_TYPE"));
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  @NonNull
  public static FileType getFileType(String name) {
    for (val value : values()) {
      if (value.name().equals(name.toUpperCase())) {
        return value;
      }
    }

    throw new IllegalArgumentException(format("Failed to resolve FileType from name '%s'", name));
  }

  @Override
  public String getId() {
    return name().toLowerCase();
  }

}
