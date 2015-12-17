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
import java.util.Map;

import lombok.Data;

@Data
public class Project implements Serializable {

  private String _project_id;
  private String primary_site;
  private String project_name;

  // These might be used later
  // private String tumour_subtype;
  // private String alias;
  // private ProjectSummary _summary;
  // private String[] primary_countries;
  // private String tumour_type;
  // private String icgc_id;
  // private String[] pubmed_ids;
  // private String[] partner_countries;

  @Data
  public static class ProjectSummary implements Serializable {

    private String _total_specimen_count;
    private String _total_donor_count;
    private String _exp_array_tested_donor_count;
    private String _sgv_tested_donor_count;
    private String[] available_experimental_analysis_performed;
    private String _pexp_tested_donor_count;
    private String _ssm_tested_donor_count;
    private String _jcn_tested_donor_count;
    private String _cnsm_tested_donor_count;
    private String _exp_seq_tested_donor_count;
    private String _stgv_tested_donor_count;
    private String _mirna_seq_tested_donor_count;
    private String _state;
    private String _total_sample_count;
    private Map<String, Integer> experimental_analysis_performed_sample_count;
    private String[] repository;
    private String _cngv_tested_donor_count;
    private String[] _available_data_type;
    private String _stsm_tested_donor_count;
    private String _meth_array_tested_donor_count;
    private String _meth_seq_tested_donor_count;
    private String _total_live_donor_count;

  }

}
