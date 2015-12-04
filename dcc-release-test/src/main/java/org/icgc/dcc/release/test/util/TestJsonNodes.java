/*
 * Copyright (c) 2013 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.release.test.util;

import static com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_COMMENTS;
import static com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_SINGLE_QUOTES;
import static com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES;
import static com.google.common.collect.ImmutableList.copyOf;

import java.io.File;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import lombok.SneakyThrows;
import lombok.val;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;

public class TestJsonNodes {

  /**
   * Allow for more liberal JSON strings to simplify literals with constants, etc.
   */
  public static final ObjectMapper MAPPER = new ObjectMapper() //
      .configure(ALLOW_UNQUOTED_FIELD_NAMES, true) //
      .configure(ALLOW_SINGLE_QUOTES, true) //
      .configure(ALLOW_COMMENTS, true);

  /**
   * Utility method that returns a {@code JsonNode} given a JSON String.
   * <p>
   * The name and use is inspired by jQuery's {@code $} function.
   * 
   * @param json
   * @return
   */
  @SneakyThrows
  public static ObjectNode $(String json) {
    return (ObjectNode) MAPPER.readTree(json);
  }

  /**
   * Utility method that returns a {@code JsonNode} given a JSON String.
   * <p>
   * The name and use is inspired by jQuery's {@code $} function.
   * 
   * @param json
   * @return
   */
  @SneakyThrows
  public static ObjectNode $(File jsonFile) {
    return (ObjectNode) MAPPER.readTree(jsonFile);
  }

  @SneakyThrows
  public static String toJson(Object object) {
    return MAPPER.writeValueAsString(object);
  }

  public static List<JsonNode> getElements(JsonNode node) {
    return copyOf(node.elements());
  }

  public static JsonNode sortFields(JsonNode json) {
    val fields = Lists.newArrayList(json.fieldNames());
    Collections.sort(fields);
    val result = MAPPER.createObjectNode();

    for (val fieldName : fields) {
      val value = json.get(fieldName);
      if (value.isObject()) {
        result.set(fieldName, sortFields(value));
      } else if (value.isArray()) {
        result.set(fieldName, sortArrayNode(value));
      } else {
        result.set(fieldName, value);
      }
    }

    return result;
  }

  private static JsonNode sortArrayNode(JsonNode value) {
    val elementsList = arrayToList(value);
    Collections.sort(elementsList, new Comparator<JsonNode>() {

      @Override
      public int compare(JsonNode left, JsonNode right) {
        val leftId = resolveIdValue(left);
        val rightId = resolveIdValue(right);
        return leftId.compareTo(rightId);
      }
    });

    val result = MAPPER.createArrayNode();
    for (val node : elementsList) {
      result.add(sortFields(node));
    }

    return result;
  }

  private static String resolveIdValue(JsonNode json) {
    if (!json.path("donor_id").isMissingNode()) {
      return json.get("donor_id").textValue();
    }

    if (!json.path("_donor_id").isMissingNode()) {
      return json.get("_donor_id").textValue();
    }

    if (!json.path("_specimen_id").isMissingNode()) {
      return json.get("_specimen_id").textValue();
    }

    if (!json.path("_sample_id").isMissingNode()) {
      return json.get("_sample_id").textValue();
    }

    if (!json.path("_gene_id").isMissingNode()) {
      val geneId = json.get("_gene_id").textValue();
      val transcript = json.path("_transcript_id");

      return transcript.isMissingNode() ? geneId : geneId + transcript.textValue();
    }

    if (!json.path("_mutation_id").isMissingNode()) {
      return json.get("_mutation_id").textValue();
    }

    if (!json.path("id").isMissingNode()) {
      return json.get("id").textValue();
    }

    if (!json.path("consequence_type").isMissingNode()) {
      val consType = json.get("consequence_type").textValue();
      val funcImpact = json.path("functional_impact_prediction_summary");

      return funcImpact.isMissingNode() ? consType : consType + funcImpact.textValue();
    }

    return "";
  }

  private static List<JsonNode> arrayToList(JsonNode value) {
    val result = Lists.<JsonNode> newArrayList();

    for (val node : value) {
      result.add(node);
    }

    return result;
  }

}
