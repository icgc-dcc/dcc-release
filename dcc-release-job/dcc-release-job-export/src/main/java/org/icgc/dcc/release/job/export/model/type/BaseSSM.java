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
package org.icgc.dcc.release.job.export.model.type;

import static org.icgc.dcc.release.job.export.model.type.Constants.CONSEQUENCE_FIELD_NAME;
import static org.icgc.dcc.release.job.export.model.type.Constants.OBSERVATION_FIELD_NAME;

import java.util.Set;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.icgc.dcc.release.core.function.AddMissingField;
import org.icgc.dcc.release.core.function.FlattenField;
import org.icgc.dcc.release.core.function.ParseObjectNode;
import org.icgc.dcc.release.core.function.ProjectFields;
import org.icgc.dcc.release.core.function.PullUpField;
import org.icgc.dcc.release.core.function.RenameFields;
import org.icgc.dcc.release.core.function.RetainFields;
import org.icgc.dcc.release.job.export.function.AddDonorIdField;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

@RequiredArgsConstructor
public abstract class BaseSSM implements Type {

  @NonNull
  private final Function<ObjectNode, Boolean> filterMaskingFunction;

  private static final String DATA_TYPE_FOLDER = "ssm";

  private static final ImmutableMap<String, String> FIRST_LEVEL_PROJECTION = ImmutableMap.<String, String> builder()
      .put("_mutation_id", "icgc_mutation_id")
      .put("_donor_id", "icgc_donor_id")
      .put("_project_id", "project_code")
      .put("chromosome", "chromosome")
      .put("chromosome_start", "chromosome_start")
      .put("chromosome_end", "chromosome_end")
      .put("chromosome_strand", "chromosome_strand")
      .put("assembly_version", "assembly_version")
      .put("mutation_type", "mutation_type")
      .put("reference_genome_allele", "reference_genome_allele")
      .put("mutated_from_allele", "mutated_from_allele")
      .put("mutated_to_allele", "mutated_to_allele")
      .put("consequence", "consequence")
      .put("observation", "observation")
      .build();

  private static final ImmutableMap<String, String> SECOND_LEVEL_PROJECTION = ImmutableMap.<String, String> builder()
      .put("donor_id", "donor_id")
      .put("_specimen_id", "icgc_specimen_id")
      .put("_sample_id", "icgc_sample_id")
      .put("_matched_sample_id", "matched_icgc_sample_id")
      .put("analyzed_sample_id", "submitted_sample_id")
      .put("matched_sample_id", "submitted_matched_sample_id")
      .put("control_genotype", "control_genotype")
      .put("tumour_genotype", "tumour_genotype")
      .put("expressed_allele", "expressed_allele")
      .put("quality_score", "quality_score")
      .put("probability", "probability")
      .put("total_read_count", "total_read_count")
      .put("mutant_allele_read_count", "mutant_allele_read_count")
      .put("verification_status", "verification_status")
      .put("verification_platform", "verification_platform")
      .put("biological_validation_status", "biological_validation_status")
      .put("biological_validation_platform", "biological_validation_platform")
      .put("platform", "platform")
      .put("experimental_protocol", "experimental_protocol")
      .put("sequencing_strategy", "sequencing_strategy")
      .put("base_calling_algorithm", "base_calling_algorithm")
      .put("alignment_algorithm", "alignment_algorithm")
      .put("variation_calling_algorithm", "variation_calling_algorithm")
      .put("other_analysis_algorithm", "other_analysis_algorithm")
      .put("seq_coverage", "seq_coverage")
      .put("raw_data_repository", "raw_data_repository")
      .put("raw_data_accession", "raw_data_accession")
      .put("initial_data_release_date", "initial_data_release_date")
      .build();

  private static final ImmutableMap<String, String> THIRD_LEVEL_PROJECTION = ImmutableMap.<String, String> builder()
      .put("consequence_type", "consequence_type")
      .put("aa_mutation", "aa_mutation")
      .put("cds_mutation", "cds_mutation")
      .put("gene_affected", "gene_affected")
      .put("transcript_affected", "transcript_affected")
      .put("gene_build_version", "gene_build_version")
      .build();

  @Override
  public JavaRDD<ObjectNode> process(JavaRDD<String> input) {
    return input
        .map(new ParseObjectNode())
        .map(new ProjectFields(FIRST_LEVEL_PROJECTION))
        .map(new AddDonorIdField())
        .map(new AddMissingField(OBSERVATION_FIELD_NAME, SECOND_LEVEL_PROJECTION.keySet()))
        .flatMap(new FlattenField(OBSERVATION_FIELD_NAME))
        .map(new PullUpField(OBSERVATION_FIELD_NAME))
        .filter(filterMaskingFunction)
        .map(new RetainFields(getFirstLevelFields()))
        .map(new RenameFields(SECOND_LEVEL_PROJECTION))
        .map(new AddMissingField(CONSEQUENCE_FIELD_NAME, THIRD_LEVEL_PROJECTION.keySet()))
        .flatMap(new FlattenField(CONSEQUENCE_FIELD_NAME))
        .map(new PullUpField(CONSEQUENCE_FIELD_NAME))
        .map(new RetainFields(getFields()))
        .map(new RenameFields(THIRD_LEVEL_PROJECTION));
  }

  @Override
  public Set<String> getFields() {
    return Sets.newHashSet(Iterables.concat(getFirstLevelFields(), THIRD_LEVEL_PROJECTION.keySet()));
  }

  @Override
  public String getTypeDirectoryName() {
    return DATA_TYPE_FOLDER;
  }

  private Set<String> getFirstLevelFields() {
    return Sets.newHashSet(Iterables.concat(FIRST_LEVEL_PROJECTION.values(), SECOND_LEVEL_PROJECTION.keySet()));
  }

}
