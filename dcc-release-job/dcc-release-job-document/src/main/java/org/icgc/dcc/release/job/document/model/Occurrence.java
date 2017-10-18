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

import org.icgc.dcc.release.core.model.Observation;

@Data
public class Occurrence implements Serializable {

  private String _donor_id;
  private String _mutation_id;
  private String _type;
  private Collection<Consequence> consequence;
  private String mutation_type;
  private String chromosome;
  private Integer chromosome_start;
  private Integer chromosome_end;
  private String chromosome_strand;
  private Collection<Observation> observation;

  private String reference_genome_allele;
  private String mutated_from_allele;
  private String mutated_to_allele;
  private String _project_id;
  private String mutation;
  private String[] consequence_type;
  private String assembly_version;
  private String genomic_region;

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

}
