/*
 * Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.release.core.model;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

import java.io.Serializable;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonInclude;

@Data
@JsonInclude(NON_NULL)
public class Observation implements Serializable {

  protected String _matched_sample_id;
  protected String _sample_id;
  protected String _specimen_id;
  protected String alignment_algorithm;
  protected String analysis_id;
  protected String analyzed_sample_id;
  protected String base_calling_algorithm;
  protected String biological_validation_platform;
  protected String biological_validation_status;
  protected String control_genotype;
  protected String experimental_protocol;
  protected String expressed_allele;
  protected String marking;
  protected String matched_sample_id;
  protected Integer mutant_allele_read_count;
  protected String observation_id;
  protected String other_analysis_algorithm;
  protected String platform;
  protected Double probability;
  protected String quality_score;
  protected String raw_data_accession;
  protected String raw_data_repository;
  protected Double seq_coverage;
  protected String sequencing_strategy;
  protected Integer total_read_count;
  protected String tumour_genotype;
  protected String variation_calling_algorithm;
  protected String verification_platform;
  protected String verification_status;
  protected boolean pcawg_flag = false;

}