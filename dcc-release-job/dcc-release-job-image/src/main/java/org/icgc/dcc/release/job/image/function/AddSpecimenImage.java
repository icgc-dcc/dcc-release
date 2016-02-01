/*
 * Copyright (c) 2014 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.release.job.image.function;

import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_DIGITAL_IMAGE_OF_STAINED_SECTION;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_SPECIMEN_ID;
import static org.icgc.dcc.release.core.util.ObjectNodes.textValue;

import java.util.Map;

import lombok.NonNull;
import lombok.val;

import org.apache.spark.api.java.function.Function;
import org.icgc.dcc.common.core.model.Programs;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * This modifies the digital_image_of_stained_section field in donor specimen to a real URL, if applicable;
 */
public class AddSpecimenImage implements Function<ObjectNode, ObjectNode> {

  /**
   * State.
   */
  private final Map<String, String> specimenUrls;
  private final boolean replaceImageUrl;

  public AddSpecimenImage(@NonNull Map<String, String> specimenUrls, @NonNull String projectName) {
    this.specimenUrls = specimenUrls;
    this.replaceImageUrl = Programs.isTCGA(projectName);
  }

  @Override
  public ObjectNode call(ObjectNode row) throws Exception {
    val specimenId = textValue(row, SUBMISSION_SPECIMEN_ID);
    val imageUrl = textValue(row, SUBMISSION_DIGITAL_IMAGE_OF_STAINED_SECTION);
    val specimenUrl = replaceImageUrl ? getSpecimenUrl(specimenId) : imageUrl;

    row.put(SUBMISSION_DIGITAL_IMAGE_OF_STAINED_SECTION, specimenUrl);

    return row;
  }

  private String getSpecimenUrl(String specimenId) {
    return specimenUrls.get(specimenId);
  }

}
