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
package org.icgc.dcc.release.job.summarize.util;

import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.model.FieldNames.AVAILABLE_DATA_TYPES;
import static org.icgc.dcc.common.core.model.FieldNames.PROJECT_SUMMARY_STATE;
import static org.icgc.dcc.common.core.model.FieldNames.TOTAL_DONOR_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.TOTAL_LIVE_DONOR_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.TOTAL_SAMPLE_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.TOTAL_SPECIMEN_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.getTestedTypeCountFieldName;
import static org.icgc.dcc.release.core.util.ObjectNodes.createObject;
import lombok.NoArgsConstructor;
import lombok.val;

import org.icgc.dcc.common.core.model.FeatureTypes.FeatureType;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

@NoArgsConstructor(access = PRIVATE)
public final class Projects {

  public static ObjectNode createDefaultProjectSummary() {
    val projectSummary = createObject();

    // Defaults
    for (FeatureType type : FeatureType.values()) {
      setTestedTypeCount(projectSummary, type, 0);
    }

    // Defaults
    setAvailableTypes(projectSummary, projectSummary.arrayNode());
    setTotalDonorCount(projectSummary, 0);
    setTotalSampleCount(projectSummary, 0);
    setTotalSpecimenCount(projectSummary, 0);
    setTotalLiveDonorCount(projectSummary, 0);
    setProjectState(projectSummary, "pending");

    return projectSummary;
  }

  private static void setProjectState(ObjectNode node, String state) {
    node.put(PROJECT_SUMMARY_STATE, state);
  }

  private static void setTotalLiveDonorCount(ObjectNode node, int donorCount) {
    node.put(TOTAL_LIVE_DONOR_COUNT, donorCount);
  }

  private static void setTestedTypeCount(ObjectNode node, FeatureType type, int count) {
    node.put(getTestedTypeCountFieldName(type), count);
  }

  private static void setAvailableTypes(ObjectNode node, JsonNode types) {
    node.put(AVAILABLE_DATA_TYPES, types);
  }

  private static void setTotalDonorCount(ObjectNode node, int donorCount) {
    node.put(TOTAL_DONOR_COUNT, donorCount);
  }

  private static void setTotalSampleCount(ObjectNode node, int sampleCount) {
    node.put(TOTAL_SAMPLE_COUNT, sampleCount);
  }

  private static void setTotalSpecimenCount(ObjectNode node, int specimenCount) {
    node.put(TOTAL_SPECIMEN_COUNT, specimenCount);
  }

}
