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
package org.icgc.dcc.etl2.job.export.function;

import lombok.val;

import org.apache.spark.api.java.function.Function;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class AddMissingSpecimen implements Function<ObjectNode, ObjectNode> {

  // TODO find Field or make into Field.
  private static final String SPECIMEN_FIELD_NAME = "specimen";
  public static final String EMPTY_SPECIMEN_VALUE =
      "[\"_specimen_id\":\"\",\"specimen_id\":\"\",\"specimen_type\":\"\",\"specimen_type_other\":\"\",\"specimen_interval\":\"\",\"specimen_donor_treatment_type\":\"\",\"specimen_donor_treatment_type_other\":\"\",\"specimen_processing\":\"\",\"specimen_processing_other\":\"\",\"specimen_storage\":\"\",\"specimen_storage_other\":\"\",\"tumour_confirmed\":\"\",\"specimen_biobank\":\"\",\"specimen_biobank_id\":\"\",\"specimen_available\":\"\",\"tumour_histological_type\":\"\",\"tumour_grading_system\":\"\",\"tumour_grade\":\"\",\"tumour_grade_supplemental\":\"\",\"tumour_stage_system\":\"\",\"tumour_stage\":\"\",\"tumour_stage_supplemental\":\"\",\"digital_image_of_stained_section\":\"\"]";

  @Override
  public ObjectNode call(ObjectNode row) {
    val specimen = row.get(SPECIMEN_FIELD_NAME);
    if (specimen == null || specimen.equals("")) {
      row.put(SPECIMEN_FIELD_NAME, EMPTY_SPECIMEN_VALUE);
    }

    return row;
  }

}