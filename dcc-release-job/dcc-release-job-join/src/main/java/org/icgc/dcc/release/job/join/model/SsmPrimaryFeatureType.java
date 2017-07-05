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
package org.icgc.dcc.release.job.join.model;

import java.io.Serializable;

import lombok.Data;

@Data
public class SsmPrimaryFeatureType implements Serializable {

  private String _mutation_id;
  private String _project_id;
  private String analysis_id;
  private String analyzed_sample_id;
  private String biological_validation_platform;
  private String biological_validation_status;
  private String chromosome;
  private Integer chromosome_end;
  private Integer chromosome_start;
  private String chromosome_strand;
  private String control_genotype;
  private String expressed_allele;
  private String marking;
  private Integer mutant_allele_read_count;
  private String mutated_from_allele;
  private String mutated_to_allele;
  private String mutation;
  private String mutation_type;
  private String observation_id;
  private Double probability;
  private String quality_score;
  private String reference_genome_allele;
  private Integer total_read_count;
  private String tumour_genotype;
  private String verification_platform;
  private String verification_status;
  private String _study;

}
