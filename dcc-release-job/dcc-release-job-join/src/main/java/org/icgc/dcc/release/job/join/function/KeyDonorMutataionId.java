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

import static org.icgc.dcc.common.core.model.FieldNames.IdentifierFieldNames.SURROGATE_MUTATION_ID;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_ANALYZED_SAMPLE_ID;
import static org.icgc.dcc.release.core.util.Keys.getKey;
import static org.icgc.dcc.release.core.util.ObjectNodes.textValue;

import java.util.Map;

import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.spark.api.java.function.Function;
import org.icgc.dcc.release.job.join.model.DonorSample;

import scala.Tuple2;

import com.fasterxml.jackson.databind.node.ObjectNode;

@RequiredArgsConstructor
public class KeyDonorMutataionId implements
    Function<Tuple2<String, Tuple2<Tuple2<ObjectNode, Iterable<ObjectNode>>, ObjectNode>>, String> {

  private final Map<String, DonorSample> donorSamples;

  @Override
  public String call(Tuple2<String, Tuple2<Tuple2<ObjectNode, Iterable<ObjectNode>>, ObjectNode>> tuple)
      throws Exception {
    val primary = tuple._2._1._1;
    val mutationId = textValue(primary, SURROGATE_MUTATION_ID);
    val sampleId = textValue(primary, SUBMISSION_ANALYZED_SAMPLE_ID);

    val donorId = donorSamples.get(sampleId);
    val key = getKey(donorId.getDonorId(), mutationId);

    return key;
  }

}