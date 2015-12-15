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

import lombok.Data;

@Data
public class Occurrence implements Serializable {

  private String _donor_id;
  private String _mutation_id;
  private String _type;
  private Collection<Consequence> consequence;
  private String mutation_type;
  private String chromosome;
  private String chromosome_start;
  private String chromosome_end;
  private String chromosome_strand;
  private Collection<Observation> observation;

  private String reference_genome_allele;
  private String mutated_from_allele;
  private String mutated_to_allele;
  private String _project_id;
  private String mutation;
  private String[] consequence_type;
  private String assembly_version;

  @Data
  public static class Consequence implements Serializable {

    private String _gene_id;
    private String consequence_type;
    private String functional_impact_prediction_summary;

    private String protein_domain_affected;
    private String gene_build_version;
    private String _transcript_id;
    private String cds_change;
    private String aa_change;
    private String aa_mutation;
    private String cds_mutation;
    private String note;
    private String gene_affected;
    private String transcript_affected;

  }

  @Data
  public static class Observation implements Serializable {

    private String raw_data_accession;
    private Integer total_read_count;
    private String sequencing_strategy;
    private String _sample_id;
    private String base_calling_algorithm;
    private String tumour_genotype;
    private String seq_coverage;
    private String analyzed_sample_id;
    private String raw_data_repository;
    private String verification_platform;
    private String _specimen_id;
    private String matched_sample_id;
    private Double probability;
    private String marking;
    private String experimental_protocol;
    private String other_analysis_algorithm;
    private String platform;
    private String control_genotype;
    private String observation_id;
    private Integer mutant_allele_read_count;
    private Double quality_score;
    private String expressed_allele;
    private String biological_validation_platform;
    private String analysis_id;
    private String verification_status;
    private String alignment_algorithm;
    private String _matched_sample_id;
    private String variation_calling_algorithm;
    private String biological_validation_status;

  }

}
