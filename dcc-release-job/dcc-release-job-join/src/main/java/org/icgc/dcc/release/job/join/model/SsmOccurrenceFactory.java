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

import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.model.FeatureTypes.FeatureType.SSM_TYPE;
import lombok.NoArgsConstructor;
import lombok.val;

import org.icgc.dcc.release.core.model.Observation;

import com.google.common.collect.Lists;

@NoArgsConstructor(access = PRIVATE)
public final class SsmOccurrenceFactory {

  public static SsmOccurrence createSsmOccurrence(SsmPrimaryFeatureType primary, SsmMetaFeatureType meta) {
    val occurrence = new SsmOccurrence();
    occurrence.set_mutation_id(primary.get_mutation_id());
    occurrence.set_project_id(primary.get_project_id());
    occurrence.setChromosome(primary.getChromosome());
    occurrence.setChromosome_start(primary.getChromosome_start());
    occurrence.setChromosome_end(primary.getChromosome_end());
    occurrence.setChromosome_strand(primary.getChromosome_strand());
    occurrence.setMutated_from_allele(primary.getMutated_from_allele());
    occurrence.setMutated_to_allele(primary.getMutated_to_allele());
    occurrence.setMutation(primary.getMutation());
    occurrence.setMutation_type(primary.getMutation_type());
    occurrence.setReference_genome_allele(primary.getReference_genome_allele());
    occurrence.set_type(SSM_TYPE.getId());
    occurrence.setAssembly_version(meta.getAssembly_version());
    occurrence.setObservation(Lists.newArrayList(createObservation(primary, meta)));

    return occurrence;
  }

  private static Observation createObservation(SsmPrimaryFeatureType primary, SsmMetaFeatureType meta) {
    val observation = new Observation();
    observation.setAlignment_algorithm(meta.getAlignment_algorithm());
    observation.setAnalysis_id(primary.getAnalysis_id());
    observation.setAnalyzed_sample_id(primary.getAnalyzed_sample_id());
    observation.setBase_calling_algorithm(meta.getBase_calling_algorithm());
    observation.setBiological_validation_platform(primary.getBiological_validation_platform());
    observation.setBiological_validation_status(primary.getBiological_validation_status());
    observation.setControl_genotype(primary.getControl_genotype());
    observation.setExperimental_protocol(meta.getExperimental_protocol());
    observation.setExpressed_allele(primary.getExpressed_allele());
    observation.setMarking(primary.getMarking());
    observation.setMatched_sample_id(meta.getMatched_sample_id());
    observation.setMutant_allele_read_count(primary.getMutant_allele_read_count());
    observation.setObservation_id(primary.getObservation_id());
    observation.setOther_analysis_algorithm(meta.getOther_analysis_algorithm());
    observation.setPlatform(meta.getPlatform());
    observation.setProbability(primary.getProbability());
    observation.setQuality_score(primary.getQuality_score());
    observation.setRaw_data_accession(meta.getRaw_data_accession());
    observation.setRaw_data_repository(meta.getRaw_data_repository());
    observation.setSeq_coverage(meta.getSeq_coverage());
    observation.setSequencing_strategy(meta.getSequencing_strategy());
    observation.setTotal_read_count(primary.getTotal_read_count());
    observation.setTumour_genotype(primary.getTumour_genotype());
    observation.setVariation_calling_algorithm(meta.getVariation_calling_algorithm());
    observation.setVerification_platform(primary.getVerification_platform());
    observation.setVerification_status(primary.getVerification_status());

    return observation;
  }

}
