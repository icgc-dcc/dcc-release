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

import static org.icgc.dcc.release.job.export.model.type.Constants.MIRNA_SEQ_TYPE_FIELD_NAME;

import java.util.Set;

import org.apache.spark.api.java.JavaRDD;
import org.icgc.dcc.release.core.function.ParseObjectNode;
import org.icgc.dcc.release.core.function.ProjectFields;
import org.icgc.dcc.release.core.function.RetainFields;
import org.icgc.dcc.release.job.export.function.AddDonorIdField;
import org.icgc.dcc.release.job.export.function.IsType;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

public class MethSeq implements Type {

  private static final String DATA_TYPE_FOLDER = "meth_seq";

  private static final ImmutableMap<String, String> FIRST_LEVEL_PROJECTION = ImmutableMap.<String, String> builder()
      .put("_donor_id", "icgc_donor_id")
      .put("_project_id", "project_code")
      .put("_specimen_id", "icgc_specimen_id")
      .put("_sample_id", "icgc_sample_id")
      .put("analyzed_sample_id", "submitted_sample_id,")
      .put("analysis_id", "analysis_id")
      .put("mirna_db", "mirna_db")
      .put("mirna_id", "mirna_id")
      .put("normalized_read_count", "normalized_read_count")
      .put("raw_read_count", "raw_read_count")
      .put("fold_change", "fold_change")
      .put("is_isomir", "is_isomir")
      .put("chromosome", "chromosome")
      .put("chromosome_start", "chromosome_start")
      .put("chromosome_end", "chromosome_end")
      .put("chromosome_strand", "chromosome_strand")
      .put("assembly_version", "assembly_version")
      .put("verification_status", "verification_status")
      .put("verification_platform", "verification_platform")
      .put("sequencing_platform", "sequencing_platform")
      .put("total_read_count", "total_read_count")
      .put("experimental_protocol", "experimental_protocol")
      .put("reference_sample_type", "reference_sample_type")
      .put("alignment_algorithm", "alignment_algorithm")
      .put("normalization_algorithm", "normalization_algorithm")
      .put("other_analysis_algorithm", "other_analysis_algorithm")
      .put("sequencing_strategy", "sequencing_strategy")
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
        .filter(new IsType(MIRNA_SEQ_TYPE_FIELD_NAME))
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
