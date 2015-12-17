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
package org.icgc.dcc.release.job.document.model;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

import lombok.Data;

@Data
public class Donor implements Serializable {

  String _donor_id;
  String _project_id;
  DonorSummary _summary;
  String disease_status_last_followup;
  String donor_age_at_diagnosis;
  String donor_age_at_enrollment;
  String donor_age_at_last_followup;
  String donor_diagnosis_icd10;
  String donor_id;
  String donor_interval_of_last_followup;
  String donor_relapse_interval;
  String donor_relapse_type;
  String donor_sex;
  String donor_survival_time;
  String donor_tumour_stage_at_diagnosis;
  String donor_tumour_stage_at_diagnosis_supplemental;
  String donor_tumour_staging_system_at_diagnosis;
  String donor_vital_status;
  Collection<Map<String, Object>> gene;

  // Added during transformations
  Project project;

  @Data
  public static class DonorSummary implements Serializable {

    Integer _affected_gene_count;
    String[] repository;
    String[] _studies;
    String[] experimental_analysis_performed;
    Map<String, Integer> experimental_analysis_performed_sample_count;
    String _age_at_diagnosis_group;
    boolean _cnsm_exists;
    String[] _available_data_type;
    boolean _jcn_exists;
    boolean _meth_array_exists;
    Integer _ssm_count;
    boolean _pexp_exists;
    boolean _stsm_exists;
    boolean _meth_seq_exists;
    boolean _exp_array_exists;
    boolean _sgv_exists;
    boolean _mirna_seq_exists;
    boolean _exp_seq_exists;
    String _state;

  }

}
