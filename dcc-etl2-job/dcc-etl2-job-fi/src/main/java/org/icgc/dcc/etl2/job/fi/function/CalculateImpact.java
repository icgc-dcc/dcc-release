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
package org.icgc.dcc.etl2.job.fi.function;

import static java.util.Collections.singleton;
import static org.icgc.dcc.common.core.fi.CompositeImpactCategory.UNKNOWN;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES_CONSEQUENCE_FUNCTIONAL_IMPACT_PREDICTION;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES_CONSEQUENCE_FUNCTIONAL_IMPACT_PREDICTION_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCE_TYPES;
import static org.icgc.dcc.etl2.core.util.ObjectNodes.MAPPER;
import static org.icgc.dcc.etl2.core.util.ObjectNodes.textValue;

import java.util.Set;

import lombok.val;

import org.apache.spark.api.java.function.Function;
import org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames;
import org.icgc.dcc.etl2.job.fi.core.FunctionalImpactCalculator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Sets;

public class CalculateImpact implements Function<ObjectNode, ObjectNode> {

  @Override
  public ObjectNode call(ObjectNode observation) throws Exception {
    val impacts = calculateImpacts(observation);
    if (!impacts.isEmpty()) {
      setImpacts(observation, impacts);
    }

    return observation;
  }

  private static Set<String> calculateImpacts(ObjectNode observation) {
    val consequences = getConsequences(observation);
    if (!hasConsequences(consequences)) {
      return singleton(UNKNOWN.name());
    }

    val impacts = Sets.<String> newHashSet();
    for (val value : consequences) {
      val consequence = (ObjectNode) value;

      val impact = calculateImpact(consequence);
      setConsequenceSummary(consequence, impact);

      impacts.add(impact);
    }

    return impacts;
  }

  private static String calculateImpact(ObjectNode consequence) {
    return FunctionalImpactCalculator.calculateImpact(
        getConsequenceType(consequence), getPrediction(consequence));
  }

  private static void setImpacts(ObjectNode observation, Set<String> impacts) {
    val summary = createSummary();
    for (val impact : impacts) {
      summary.add(impact.toString());
    }

    setSummary(observation, summary);
  }

  private static JsonNode getPrediction(final com.fasterxml.jackson.databind.node.ObjectNode consequence) {
    return consequence.get(OBSERVATION_CONSEQUENCES_CONSEQUENCE_FUNCTIONAL_IMPACT_PREDICTION);
  }

  private static boolean hasConsequences(ArrayNode consequences) {
    return null != consequences && consequences.isArray() && consequences.size() > 0;
  }

  private static ArrayNode getConsequences(ObjectNode observation) {
    return (ArrayNode) observation.get(LoaderFieldNames.CONSEQUENCE_ARRAY_NAME);
  }

  private static String getConsequenceType(ObjectNode consequence) {
    return textValue(consequence, OBSERVATION_CONSEQUENCE_TYPES);
  }

  private static void setConsequenceSummary(ObjectNode consequence, String impact) {
    consequence.put(OBSERVATION_CONSEQUENCES_CONSEQUENCE_FUNCTIONAL_IMPACT_PREDICTION_SUMMARY, impact);
  }

  private static ArrayNode createSummary() {
    return MAPPER.createArrayNode();
  }

  private static void setSummary(ObjectNode observation, ArrayNode summary) {
    observation.put(OBSERVATION_CONSEQUENCES_CONSEQUENCE_FUNCTIONAL_IMPACT_PREDICTION_SUMMARY, summary);
  }

}