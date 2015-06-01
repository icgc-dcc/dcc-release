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

import static org.icgc.dcc.etl2.job.export.model.type.Constants.JCN_TYPE_FIELD_NAME;

import java.util.Set;

import lombok.RequiredArgsConstructor;

import org.apache.spark.api.java.JavaRDD;
import org.icgc.dcc.etl2.core.function.ParseObjectNode;
import org.icgc.dcc.etl2.core.function.ProjectFields;
import org.icgc.dcc.etl2.core.function.RetainFields;
import org.icgc.dcc.etl2.job.export.function.AddDonorIdField;
import org.icgc.dcc.etl2.job.export.function.IsType;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

@RequiredArgsConstructor
public class JCNDataType implements DataType {

  private final String DATA_TYPE_FOLDER = "jcn";

  private static final ImmutableMap<String, String> FIRST_LEVEL_PROJECTION = ImmutableMap.<String, String> builder()
      .put("_donor_id", "icgc_donor_id")
      .put("_project_id", "project_code")
      .put("_specimen_id", "icgc_specimen_id")
      .put("_sample_id", "icgc_sample_id")
      .put("analyzed_sample_id", "submitted_sample_id,")
      .put("analysis_id", "analysis_id")
      .put("junction_id", "junction_id")
      .put("gene_stable_id", "gene_stable_id")
      .put("gene_chromosome", "gene_chromosome")
      .put("gene_strand", "gene_strand")
      .put("gene_start", "gene_start")
      .put("gene_end", "gene_end")
      .put("assembly_version", "assembly_version")
      .put("second_gene_stable_id", "second_gene_stable_id")
      .put("exon1_chromosome", "exon1_chromosome")
      .put("exon1_number_bases", "exon1_number_bases")
      .put("exon1_end", "exon1_end")
      .put("exon1_strand", "exon1_strand")
      .put("exon2_chromosome", "exon2_chromosome")
      .put("exon2_number_bases", "exon2_number_bases")
      .put("exon2_start", "exon2_start")
      .put("exon2_strand", "exon2_strand")
      .put("is_fusion_gene", "is_fusion_gene")
      .put("is_novel_splice_form", "is_novel_splice_form")
      .put("junction_seq", "junction_seq")
      .put("junction_type", "junction_type")
      .put("junction_read_count", "junction_read_count")
      .put("quality_score", "quality_score")
      .put("probability", "probability")
      .put("verification_status", "verification_status")
      .put("verification_platform", "verification_platform")
      .put("gene_build_version", "gene_build_version")
      .put("platform", "platform")
      .put("experimental_protocol", "experimental_protocol")
      .put("base_calling_algorithm", "base_calling_algorithm")
      .put("alignment_algorithm", "alignment_algorithm")
      .put("normalization_algorithm", "normalization_algorithm")
      .put("other_analysis_algorithm", "other_analysis_algorithm")
      .put("sequencing_strategy", "sequencing_strategy")
      .put("seq_coverage", "seq_coverage")
      .put("raw_data_repository", "raw_data_repository")
      .put("raw_data_accession", "raw_data_accession")
      .build();

  private static final ImmutableMap<String, String> SECOND_LEVEL_PROJECTION = ImmutableMap.<String, String> builder()
      .put("donor_id", "donor_id")
      .build();

  @Override
  public JavaRDD<ObjectNode> process(JavaRDD<String> input) {
    return input
        .map(new ParseObjectNode())
        .filter(new IsType(JCN_TYPE_FIELD_NAME))
        .map(new ProjectFields(FIRST_LEVEL_PROJECTION))
        .map(new AddDonorIdField())
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
