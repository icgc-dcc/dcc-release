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

import static org.icgc.dcc.etl2.job.export.model.Constants.CNSM_TYPE_FIELD_VALUE;
import static org.icgc.dcc.etl2.job.export.model.Constants.CONSEQUENCE_FIELD_NAME;

import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.icgc.dcc.etl2.core.function.FlattenField;
import org.icgc.dcc.etl2.core.function.Identity;
import org.icgc.dcc.etl2.core.function.ProjectFields;
import org.icgc.dcc.etl2.core.function.RetainFields;
import org.icgc.dcc.etl2.job.export.function.AddMissingConsequence;
import org.icgc.dcc.etl2.job.export.function.isType;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class CNSMDataType implements DataType {

  private static final String SECOND_LEVEL_FIELDNAME = CONSEQUENCE_FIELD_NAME;

  private static final ImmutableMap<String, String> FIRST_LEVEL_PROJECTION = ImmutableMap.<String, String> builder()
      .put("_donor_id", "icgc_donor_id")
      .put("_project_id", "project_code")
      .put("_specimen_id", "icgc_specimen_id")
      .put("_sample_id", "icgc_sample_id")
      .put("_matched_sample_id", "matched_icgc_sample_id")
      .put("analyzed_sample_id", "submitted_sample_id")
      .put("matched_sample_id", "submitted_matched_sample_id")
      .put("mutation_type", "mutation_type")
      .put("copy_number", "copy_number")
      .put("segment_mean", "segment_mean")
      .put("segment_median", "segment_median")
      .put("chromosome", "chromosome")
      .put("chromosome_start", "chromosome_start")
      .put("chromosome_end", "chromosome_end")
      .put("assembly_version", "assembly_version")
      .put("chromosome_start_range", "chromosome_start_range")
      .put("chromosome_end_range", "chromosome_end_range")
      .put("start_probe_id", "start_probe_id")
      .put("end_probe_id", "end_probe_id")
      .put("sequencing_strategy", "sequencing_strategy")
      .put("quality_score", "quality_score")
      .put("probability", "probability")
      .put("is_annotated", "is_annotated")
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
      .put("gene_affected", "gene_affected")
      .put("transcript_affected", "transcript_affected")
      .put("gene_build_version", "gene_build_version")
      .build();

  private static final List<String> ALL_FIELDS = Lists.newArrayList(
      Iterables.concat(FIRST_LEVEL_PROJECTION.values(),
          SECOND_LEVEL_PROJECTION.values()));

  @Override
  public Function<ObjectNode, Boolean> primaryTypeFilter() {
    return new isType(CNSM_TYPE_FIELD_VALUE);
  }

  @Override
  public Function<ObjectNode, ObjectNode> firstLevelProjectFields() {

    return new ProjectFields(FIRST_LEVEL_PROJECTION);
  }

  @Override
  public Function<ObjectNode, ObjectNode> allLevelFilterFields() {
    return new RetainFields(ALL_FIELDS);
  }

  @Override
  public Function<ObjectNode, ObjectNode> secondLevelRenameFields() {
    return new Identity();
  }

  @Override
  public FlatMapFunction<ObjectNode, ObjectNode> secondLevelFlatten() {
    return new FlattenField(SECOND_LEVEL_FIELDNAME);
  }

  @Override
  public Function<ObjectNode, ObjectNode> secondLevelAddMissing() {
    return new AddMissingConsequence();
  }

}
