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
package org.icgc.dcc.etl2.job.export.model.type;

import static org.icgc.dcc.etl2.job.export.model.type.Constants.CONSEQUENCE_FIELD_NAME;
import static org.icgc.dcc.etl2.job.export.model.type.Constants.STSM_TYPE_FIELD_NAME;

import java.util.Set;

import org.apache.spark.api.java.JavaRDD;
import org.icgc.dcc.etl2.core.function.AddMissingField;
import org.icgc.dcc.etl2.core.function.FlattenField;
import org.icgc.dcc.etl2.core.function.ParseObjectNode;
import org.icgc.dcc.etl2.core.function.ProjectFields;
import org.icgc.dcc.etl2.core.function.PullUpField;
import org.icgc.dcc.etl2.core.function.RetainFields;
import org.icgc.dcc.etl2.job.export.function.AddDonorIdField;
import org.icgc.dcc.etl2.job.export.function.IsType;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

public class STSM implements Type {

  private final String DATA_TYPE_FOLDER = "stsm";

  private static final ImmutableMap<String, String> FIRST_LEVEL_PROJECTION = ImmutableMap.<String, String> builder()
      .put("_donor_id", "icgc_donor_id")
      .put("_project_id", "project_code")
      .put("_specimen_id", "icgc_specimen_id")
      .put("_sample_id", "icgc_sample_id")
      .put("analyzed_sample_id", "submitted_sample_id,")
      .put("matched_sample_id", "submitted_matched_sample_id")
      .put("variant_type ", "variant_type")
      .put("sv_id", "sv_id")
      .put("placement", "placement")
      .put("annotation", "annotation")
      .put("interpreted_annotation", "interpreted_annotation")
      .put("chr_from", "chr_from")
      .put("chr_from_bkpt", "chr_from_bkpt")
      .put("chr_from_strand", "chr_from_strand")
      .put("chr_from_range", "chr_from_range")
      .put("chr_from_flanking_seq", "chr_from_flanking_seq")
      .put("chr_to", "chr_to")
      .put("chr_to_bkpt", "chr_to_bkpt")
      .put("chr_to_strand", "chr_to_strand")
      .put("chr_to_range", "chr_to_range")
      .put("chr_to_flanking_seq", "chr_to_flanking_seq")
      .put("assembly_version", "assembly_version")
      .put("sequencing_strategy", "sequencing_strategy")
      .put("microhomology_sequence", "microhomology_sequence")
      .put("non_templated_sequence", "non_templated_sequence")
      .put("evidence", "evidence")
      .put("quality_score", "quality_score")
      .put("probability", "probability")
      .put("zygosity", "zygosity")
      .put("verification_status", "verification_status")
      .put("verification_platform", "verification_platform")
      .put("consequence", "consequences")
      .put("platform", "platform")
      .put("experimental_protocol", "experimental_protocol")
      .put("base_calling_algorithm", "base_calling_algorithm")
      .put("alignment_algorithm", "alignment_algorithm")
      .put("variation_calling_algorithm", "variation_calling_algorithm")
      .put("other_analysis_algorithm", "other_analysis_algorithm")
      .put("seq_coverage", "seq_coverage")
      .put("raw_data_repository", "raw_data_repository")
      .put("raw_data_accession", "raw_data_accession")
      .build();

  private static final ImmutableMap<String, String> SECOND_LEVEL_PROJECTION = ImmutableMap.<String, String> builder()
      .put("donor_id", "donor_id")
      .put("gene_affected_by_bkpt_from", "gene_affected_by_bkpt_from")
      .put("gene_affected_by_bkpt_to", "gene_affected_by_bkpt_to")
      .put("transcript_affected_by_bkpt_from", "transcript_affected_by_bkpt_from")
      .put("transcript_affected_by_bkpt_to", "transcript_affected_by_bkpt_to")
      .put("bkpt_from_context", "bkpt_from_context")
      .put("bkpt_to_context", "bkpt_to_context")
      .put("gene_build_version", "gene_build_version")
      .build();

  @Override
  public JavaRDD<ObjectNode> process(JavaRDD<String> input) {
    return input
        .map(new ParseObjectNode())
        .filter(new IsType(STSM_TYPE_FIELD_NAME))
        .map(new ProjectFields(FIRST_LEVEL_PROJECTION))
        .map(new AddDonorIdField())
        .map(new AddMissingField(CONSEQUENCE_FIELD_NAME, SECOND_LEVEL_PROJECTION.keySet()))
        .flatMap(new FlattenField(CONSEQUENCE_FIELD_NAME))
        .map(new PullUpField(CONSEQUENCE_FIELD_NAME))
        .map(new RetainFields(getFields()));
  }

  @Override
  public Set<String> getFields() {
    return Sets.newHashSet(Iterables.concat(FIRST_LEVEL_PROJECTION.values(), SECOND_LEVEL_PROJECTION.keySet()));
  }

  @Override
  public String getTypeDirectoryName() {
    return DATA_TYPE_FOLDER;
  }

}
