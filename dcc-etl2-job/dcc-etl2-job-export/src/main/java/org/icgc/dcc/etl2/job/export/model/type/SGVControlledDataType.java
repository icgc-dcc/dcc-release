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

import static org.icgc.dcc.etl2.job.export.model.Constants.CONSEQUENCE_FIELD_NAME;

import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.icgc.dcc.etl2.core.function.FlattenField;
import org.icgc.dcc.etl2.core.function.ProjectFields;
import org.icgc.dcc.etl2.core.function.RenameFields;
import org.icgc.dcc.etl2.core.function.RetainFields;
import org.icgc.dcc.etl2.job.export.function.AddMissingConsequence;
import org.icgc.dcc.etl2.job.export.function.All;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class SGVControlledDataType implements DataType {

  private static final String SECOND_LEVEL_FIELDNAME = CONSEQUENCE_FIELD_NAME;

  private static final ImmutableMap<String, String> FIRST_LEVEL_PROJECTION = ImmutableMap.<String, String> builder()
      .put("_donor_id", "icgc_donor_id")
      .put("_project_id", "project_code")
      .put("_specimen_id", "icgc_specimen_id")
      .put("_sample_id", "icgc_sample_id")
      .put("analyzed_sample_id", "submitted_sample_id")
      .put("analysis_id", "analysis_id")
      .put("chromosome", "chromosome")
      .put("chromosome_start", "chromosome_start")
      .put("chromosome_end", "chromosome_end")
      .put("chromosome_strand", "chromosome_strand")
      .put("assembly_version", "assembly_version")
      .put("variant_type", "variant_type")
      .put("reference_genome_allele", "reference_genome_allele")
      .put("genotype", "genotype")
      .put("variant_allele", "variant_allele")
      .put("quality_score", "quality_score")
      .put("probability", "probability")
      .put("total_read_count", "total_read_count")
      .put("variant_allele_read_count", "variant_allele_read_count")
      .put("verification_status", "verification_status")
      .put("verification_platform", "verification_platform")
      .put("consequence", "consequences")
      .put("platform", "platform")
      .put("experimental_protocol", "experimental_protocol")
      .put("base_calling_algorithm", "base_calling_algorithm")
      .put("alignment_algorithm", "alignment_algorithm")
      .put("variation_calling_algorithm", "variation_calling_algorithm")
      .put("other_analysis_algorithm", "other_analysis_algorithm")
      .put("sequencing_strategy", "sequencing_strategy")
      .put("seq_coverage", "seq_coverage")
      .put("raw_data_repository", "raw_data_repository")
      .put("raw_data_accession", "raw_data_accession")
      .put("note", "note")
      .build();

  private static final ImmutableMap<String, String> SECOND_LEVEL_PROJECTION = ImmutableMap.<String, String> builder()
      .put("consequence_type", "consequence_type")
      .put("aa_change", "aa_change")
      .put("cds_change", "cds_change")
      .put("gene_affected", "gene_affected")
      .put("transcript_affected", "transcript_affected")
      .build();

  private static final List<String> ALL_FIELDS = Lists.newArrayList(
      Iterables.concat(FIRST_LEVEL_PROJECTION.values(),
          SECOND_LEVEL_PROJECTION.values()));

  @Override
  public Function<ObjectNode, Boolean> primaryTypeFilter() {

    return new All();
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

    return new RenameFields(SECOND_LEVEL_PROJECTION);
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
