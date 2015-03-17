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
package org.icgc.dcc.etl2.core.job;

import java.util.Optional;

import lombok.val;

import org.icgc.dcc.common.core.model.FileTypes;

public enum FileType {

  //
  // Clinical
  //

  DONOR,
  SPECIMEN,
  SAMPLE,

  //
  // Optionals
  //

  BIOMARKER,
  FAMILY,
  EXPOSURE,
  SURGERY,
  THERAPY,

  //
  // Feature Types
  //

  SSM_M,
  SSM_P,
  SSM_S,

  CNSM_M,
  CNSM_P,
  CNSM_S,

  STSM_M,
  STSM_P,
  STSM_S,

  SGV_M,
  SGV_P,
  SGV_S,

  CNGV_M,
  CNGV_P,
  CNGV_S,

  STGV_M,
  STGV_P,
  STGV_S,

  PEXP_M,
  PEXP_P,

  METH_ARRAY_M,
  METH_ARRAY_PROBES,
  METH_ARRAY_P,

  METH_SEQ_M,
  METH_SEQ_P,

  MIRNA_SEQ_M,
  MIRNA_SEQ_P,

  JCN_M,
  JCN_P,

  EXP_ARRAY_M,
  EXP_ARRAY_P,

  EXP_SEQ_M,
  EXP_SEQ_P,

  //
  // New
  //

  CLINICAL,

  DONOR_ORPHANED,
  SPECIMEN_ORPHANED,
  SAMPLE_ORPHANED,

  DONOR_SURROGATE_KEY,
  SPECIMEN_SURROGATE_KEY_IMAGE,
  SAMPLE_SURROGATE_KEY,

  SSM_P_MASKED,
  SSM_P_MASKED_SURROGATE_KEY,
  SPECIMEN_SURROGATE_KEY,

  SGV_P_MASKED,

  RELEASE,
  PROJECT,
  GENE,
  GENE_SET,
  OBSERVATION,
  MUTATION,

  OBSERVATION_FATHMM,
  OBSERVATION_FI,

  DONOR_GENE_OBSERVATION_SUMMARY,

  EXPORT_INPUT;

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
