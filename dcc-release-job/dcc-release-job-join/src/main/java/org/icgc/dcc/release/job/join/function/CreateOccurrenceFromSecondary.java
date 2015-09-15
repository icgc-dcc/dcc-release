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

import static org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames.CONSEQUENCE_ARRAY_NAME;
import static org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames.GENE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames.TRANSCRIPT_ID;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_ANALYZED_SAMPLE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_GENE_AFFECTED;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_ANALYSIS_ID;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_TRANSCRIPT_AFFECTED;
import static org.icgc.dcc.common.core.util.Jackson.DEFAULT;
import lombok.val;

import org.apache.spark.api.java.function.Function;
import org.icgc.dcc.release.core.util.ObjectNodes;

import scala.Tuple2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;

public class CreateOccurrenceFromSecondary implements
    Function<Tuple2<String, Tuple2<ObjectNode, Optional<Iterable<ObjectNode>>>>, ObjectNode> {

  private static final ObjectMapper MAPPER = DEFAULT;

  @Override
  public ObjectNode call(Tuple2<String, Tuple2<ObjectNode, Optional<Iterable<ObjectNode>>>> tuple) throws Exception {
    val primary = tuple._2._1;
    val secondaries = tuple._2._2;
    primary.put(CONSEQUENCE_ARRAY_NAME, createConsequences(secondaries));

    return primary;
  }

  private static ArrayNode createConsequences(Optional<Iterable<ObjectNode>> secondaries) {
    val consequences = MAPPER.createArrayNode();

    if (secondaries.isPresent()) {
      for (val secondary : secondaries.get()) {
        trimConsequence(secondary);
        enrichConsequence(secondary);
        consequences.add(secondary);
      }
    }

    return consequences;
  }

  private static void enrichConsequence(ObjectNode secondary) {
    secondary.put(GENE_ID, ObjectNodes.textValue(secondary, SUBMISSION_GENE_AFFECTED));
    secondary.put(TRANSCRIPT_ID, ObjectNodes.textValue(secondary, SUBMISSION_TRANSCRIPT_AFFECTED));
  }

  private static void trimConsequence(ObjectNode secondary) {
    secondary.remove(SUBMISSION_OBSERVATION_ANALYSIS_ID);
    secondary.remove(SUBMISSION_ANALYZED_SAMPLE_ID);
  }

}
