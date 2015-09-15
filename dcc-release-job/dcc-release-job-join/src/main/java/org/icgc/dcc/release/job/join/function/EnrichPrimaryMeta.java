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
package org.icgc.dcc.release.job.join.function;

import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.IdentifierFieldNames.SURROGATE_DONOR_ID;
import static org.icgc.dcc.common.core.model.FieldNames.IdentifierFieldNames.SURROGATE_SAMPLE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.IdentifierFieldNames.SURROGATE_SPECIMEN_ID;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_ANALYZED_SAMPLE_ID;
import static org.icgc.dcc.release.core.util.ObjectNodes.textValue;

import java.util.Map;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.spark.api.java.function.Function;
import org.icgc.dcc.release.job.join.model.DonorSample;

import com.fasterxml.jackson.databind.node.ObjectNode;

@RequiredArgsConstructor
public class EnrichPrimaryMeta implements Function<ObjectNode, ObjectNode> {

  @NonNull
  private final String type;
  @NonNull
  private final Map<String, DonorSample> donorSamples;

  @Override
  public ObjectNode call(ObjectNode node) throws Exception {
    node.put(OBSERVATION_TYPE, type);

    val sampleId = textValue(node, SUBMISSION_ANALYZED_SAMPLE_ID);
    val sample = donorSamples.get(sampleId);
    if (sample != null) {
      node.put(SURROGATE_SPECIMEN_ID, sample.getSpecimenId());
      node.put(SURROGATE_SAMPLE_ID, sample.getSampleId());
      node.put(SURROGATE_DONOR_ID, sample.getDonorId());
    }

    return node;
  }

}
