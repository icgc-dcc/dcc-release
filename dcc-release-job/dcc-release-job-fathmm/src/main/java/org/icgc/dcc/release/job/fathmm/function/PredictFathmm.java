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
package org.icgc.dcc.release.job.fathmm.function;

import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES_AA_MUTATION;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES_CONSEQUENCE_FUNCTIONAL_IMPACT_PREDICTION;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES_TRANSCRIPT_ID;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCE_TYPES;
import static org.icgc.dcc.release.core.util.ObjectNodes.MAPPER;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.function.Function;
import org.icgc.dcc.common.core.model.ConsequenceType;
import org.icgc.dcc.release.job.fathmm.core.FathmmPredictor;
import org.icgc.dcc.release.job.fathmm.repository.FathmmRepository;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.BiMap;
import com.google.common.collect.Lists;

@Slf4j
@RequiredArgsConstructor
public class PredictFathmm implements Function<ObjectNode, ObjectNode>, Closeable {

  /**
   * Configuration.
   */
  @NonNull
  private final String fathmmRepositoryUrl;
  @NonNull
  private final BiMap<String, String> transcripts;

  /**
   * State.
   */
  private transient FathmmPredictor predictor;

  @Override
  public ObjectNode call(ObjectNode observation) throws Exception {
    val consequences = (ArrayNode) observation.get(OBSERVATION_CONSEQUENCES);
    val consequenceList = Lists.<JsonNode> newArrayList();

    if (null != consequences && consequences.isArray()) {
      for (val consequence : consequences) {
        consequenceList.add(consequence);

        val aaMutation = consequence.path(OBSERVATION_CONSEQUENCES_AA_MUTATION);
        val transcriptId = consequence.path(OBSERVATION_CONSEQUENCES_TRANSCRIPT_ID);
        val consequenceType = consequence.path(OBSERVATION_CONSEQUENCE_TYPES);

        if (!hasValue(aaMutation) || !hasValue(transcriptId)
            || !consequenceType.textValue().equals(ConsequenceType.MISSENSE_VARIANT.getId())) {
          continue;
        }

        val translationIdStr = transcripts.get(transcriptId.textValue());
        val aaMutationStr = aaMutation.textValue();
        if (null == translationIdStr) {
          continue;
        }

        try {
          val fathmmNode = calculateFATHMM(translationIdStr, aaMutationStr);
          if (fathmmNode != null) {
            if (consequence.get(OBSERVATION_CONSEQUENCES_CONSEQUENCE_FUNCTIONAL_IMPACT_PREDICTION) == null) {
              ((ObjectNode) consequence).set(OBSERVATION_CONSEQUENCES_CONSEQUENCE_FUNCTIONAL_IMPACT_PREDICTION,
                  JsonNodeFactory.instance.objectNode());
            }
            ((ObjectNode) consequence.get(OBSERVATION_CONSEQUENCES_CONSEQUENCE_FUNCTIONAL_IMPACT_PREDICTION)).set(
                "fathmm", fathmmNode);
          }
        } catch (Exception e) {
          log.error("Failed to predict Fathmm for Consequence {} of Observation : {}", consequence, observation);
          throw e;
        }

      }
    }

    return observation;
  }

  private boolean hasValue(JsonNode aaMutation) {
    return aaMutation != null && !aaMutation.isMissingNode() && !aaMutation.isNull();
  }

  @Override
  public void close() throws IOException {
    if (predictor != null) {
      predictor.close();
    }
  }

  private ObjectNode calculateFATHMM(String translationIdStr, String aaMutationStr) {
    ObjectNode fathmmNode = null;
    val result = predict(translationIdStr, aaMutationStr);
    if (!result.isEmpty() && result.get("Score") != null) {
      fathmmNode = MAPPER.createObjectNode();
      fathmmNode.put("score", result.get("Score"));
      fathmmNode.put("prediction", result.get("Prediction"));
      fathmmNode.put("algorithm", "fathmm");
    }

    return fathmmNode;
  }

  private Map<String, String> predict(String translationIdStr, String aaMutationStr) {
    if (predictor == null) {
      val repository = new FathmmRepository(fathmmRepositoryUrl);
      predictor = new FathmmPredictor(repository);
    }

    return predictor.predict(translationIdStr, aaMutationStr);
  }

}