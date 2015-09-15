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

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames.AVAILABLE_RAW_SEQUENCE_DATA;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_ANALYZED_SAMPLE_ID;
import static org.icgc.dcc.release.core.util.ObjectNodes.textValue;
import static org.icgc.dcc.release.core.util.Tuples.tuple;
import static org.icgc.dcc.release.job.join.utils.JsonNodes.populateArrayNode;
import lombok.NoArgsConstructor;
import lombok.val;
import scala.Tuple2;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;

@NoArgsConstructor(access = PRIVATE)
public final class CombineSampleFunctions {

  public static Tuple2<String, ObjectNode> pairSampleId(ObjectNode node) {
    return tuple(getSampleId(node), node);
  }

  public static ObjectNode combineSample(Tuple2<String, Tuple2<ObjectNode, Optional<Iterable<ObjectNode>>>> tuple) {
    val sample = tuple._2._1;
    val rawSeqData = tuple._2._2;

    val rawSeqDataArray = sample.withArray(AVAILABLE_RAW_SEQUENCE_DATA);
    if (rawSeqData.isPresent()) {
      populateArrayNode(rawSeqDataArray, rawSeqData.get(), CombineSampleFunctions::trimRawSequenceData);
    }

    return sample;
  }

  public static String extractSampleId(ObjectNode node) {
    return getSampleId(node);
  }

  private static String getSampleId(ObjectNode node) {
    val sampleId = textValue(node, SUBMISSION_ANALYZED_SAMPLE_ID);
    checkState(!isNullOrEmpty(sampleId), "Failed to resolve %s for %s", SUBMISSION_ANALYZED_SAMPLE_ID, node);
    return sampleId;
  }

  private static ObjectNode trimRawSequenceData(ObjectNode node) {
    node.remove(SUBMISSION_ANALYZED_SAMPLE_ID);

    return node;
  }

}
