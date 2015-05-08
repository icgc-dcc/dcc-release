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

import static org.icgc.dcc.etl2.job.export.model.Constants.SPECIMEN_FIELD_NAME;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.icgc.dcc.etl2.core.function.AddMissingField;
import org.icgc.dcc.etl2.core.function.FlattenField;
import org.icgc.dcc.etl2.core.function.ParseObjectNode;
import org.icgc.dcc.etl2.core.function.ProjectFields;
import org.icgc.dcc.etl2.core.function.PullUpField;
import org.icgc.dcc.etl2.core.function.RenameFields;
import org.icgc.dcc.etl2.core.function.RetainFields;
import org.icgc.dcc.etl2.job.export.function.AddDonorIdField;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

@RequiredArgsConstructor
public class ClinicalDataType implements DataType {

  @NonNull
  private final JavaSparkContext sparkContext;

  private static final ImmutableMap<String, String> FIRST_LEVEL_PROJECTION = ImmutableMap.<String, String> builder()
      .put("_donor_id", "icgc_donor_id")
      .put("_project_id", "project_code")
      .put("donor_id", "submitted_donor_id")
      .put("donor_sex", "donor_sex")
      .put("donor_vital_status", "donor_vital_status")
      .put("disease_status_last_followup", "disease_status_last_followup")
      .put("donor_relapse_type", "donor_relapse_type")
      .put("donor_age_at_diagnosis", "donor_age_at_diagnosis")
      .put("donor_age_at_enrollment", "donor_age_at_enrollment")
      .put("donor_age_at_last_followup", "donor_age_at_last_followup")
      .put("donor_relapse_interval", "donor_relapse_interval")
      .put("donor_diagnosis_icd10", "donor_diagnosis_icd10")
      .put("donor_tumour_staging_system_at_diagnosis", "donor_tumour_staging_system_at_diagnosis")
      .put("donor_tumour_stage_at_diagnosis", "donor_tumour_stage_at_diagnosis")
      .put("donor_tumour_stage_at_diagnosis_supplemental", "donor_tumour_stage_at_diagnosis_supplemental")
      .put("donor_survival_time", "donor_survival_time")
      .put("donor_interval_of_last_followup", "donor_interval_of_last_followup")
      .put(SPECIMEN_FIELD_NAME, SPECIMEN_FIELD_NAME)
      .build();

  private static final ImmutableMap<String, String> SECOND_LEVEL_PROJECTION = ImmutableMap.<String, String> builder()
      .put("donor_id", "donor_id")
      .put("_specimen_id", "icgc_specimen_id")
      .put("specimen_id", "submitted_specimen_id")
      .put("specimen_type", "specimen_type")
      .put("specimen_type_other", "specimen_type_other")
      .put("specimen_interval", "specimen_interval")
      .put("specimen_donor_treatment_type", "specimen_donor_treatment_type")
      .put("specimen_donor_treatment_type_other", "specimen_donor_treatment_type_other")
      .put("specimen_processing", "specimen_processing")
      .put("specimen_processing_other", "specimen_processing_other")
      .put("specimen_storage", "specimen_storage")
      .put("specimen_storage_other", "specimen_storage_other")
      .put("tumour_confirmed", "tumour_confirmed")
      .put("specimen_biobank", "specimen_biobank")
      .put("specimen_biobank_id", "specimen_biobank_id")
      .put("specimen_available", "specimen_available")
      .put("tumour_histological_type", "tumour_histological_type")
      .put("tumour_grading_system", "tumour_grading_system")
      .put("tumour_grade", "tumour_grade")
      .put("tumour_grade_supplemental", "tumour_grade_supplemental")
      .put("tumour_stage_system", "tumour_stage_system")
      .put("tumour_stage", "tumour_stage")
      .put("tumour_stage_supplemental", "tumour_stage_supplemental")
      .put("digital_image_of_stained_section", "digital_image_of_stained_section")
      .put("percentage_cellularity", "percentage_cellularity")
      .put("level_of_cellularity", "level_of_cellularity")
      .build();

  @Override
  public JavaRDD<ObjectNode> process(Path inputPath) {
    return process(sparkContext.textFile(inputPath.toString()));
  }

  @Override
  public JavaRDD<ObjectNode> process(JavaRDD<String> input) {
    return input
        .map(new ParseObjectNode())
        .map(new ProjectFields(FIRST_LEVEL_PROJECTION))
        .map(new AddDonorIdField())
        .map(new AddMissingField(SPECIMEN_FIELD_NAME, SECOND_LEVEL_PROJECTION.keySet()))
        .flatMap(new FlattenField(SPECIMEN_FIELD_NAME))
        .map(new PullUpField(SPECIMEN_FIELD_NAME))
        .map(
            new RetainFields(Lists.newArrayList((Iterables.concat(FIRST_LEVEL_PROJECTION.values(),
                SECOND_LEVEL_PROJECTION.keySet())))))
        .map(new RenameFields(SECOND_LEVEL_PROJECTION));
  }

}
