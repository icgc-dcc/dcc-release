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
package org.icgc.dcc.etl2.core.job;

import java.util.Optional;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.icgc.dcc.common.core.model.FileTypes;

@RequiredArgsConstructor
public enum FileType {

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

  CNSM_M(true),
  CNSM_P(true),
  CNSM_S(true),

  STSM_M(true),
  STSM_P(true),
  STSM_S(true),

  SGV_M(true),
  SGV_P(true),
  SGV_S(true),

  CNGV_M(true),
  CNGV_P(true),
  CNGV_S(true),

  STGV_M(true),
  STGV_P(true),
  STGV_S(true),

  PEXP_M(true),
  PEXP_P(true),

  METH_ARRAY_M(true),
  METH_ARRAY_PROBES(true),
  METH_ARRAY_P(true),

  METH_SEQ_M(true),
  METH_SEQ_P(true),

  MIRNA_SEQ_M(true),
  MIRNA_SEQ_P(true),

  JCN_M(true),
  JCN_P(true),

  EXP_ARRAY_M(true),
  EXP_ARRAY_P(true),

  EXP_SEQ_M(true),
  EXP_SEQ_P(true),

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

  OBSERVATION_FATHMM(true),
  OBSERVATION_FI(true),

  DONOR_GENE_OBSERVATION_SUMMARY(true),

  EXPORT_INPUT(true);

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

}