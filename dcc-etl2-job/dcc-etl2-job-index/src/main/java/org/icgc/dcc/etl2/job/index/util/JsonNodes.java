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
package org.icgc.dcc.etl2.job.index.util;

import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.unmodifiableList;
import static lombok.AccessLevel.PRIVATE;

import java.util.List;

import lombok.NoArgsConstructor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

@NoArgsConstructor(access = PRIVATE)
public final class JsonNodes {

  /**
   * Constants.
   */
  private static final String DEFAULT_MISSING_VALUE = "";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static String normalizeTextValue(JsonNode node, String property) {
    return emptyToNull(node.path(property).textValue());
  }

  public static List<String> textValues(String fieldName, Iterable<JsonNode> nodes) {
    List<String> list = newArrayList();
    for (JsonNode node : nodes) {
      list.add(node.path(fieldName).textValue());
    }

    return unmodifiableList(list);
  }

  public static boolean isBlank(JsonNode node, String property) {
    return normalizeTextValue(node, property) == null;
  }

  public static boolean isEmpty(JsonNode node) {
    return node.size() == 0;
  }

  public static JsonNode defaultObject(JsonNode jsonNode) {
    return jsonNode == null ? MAPPER.createObjectNode() : jsonNode;
  }

  public static ObjectNode defaultObject(ObjectNode objectNode) {
    return objectNode == null ? MAPPER.createObjectNode() : objectNode;
  }

  public static void defaultNull(JsonNode jsonNode, String fieldName) {
    if (jsonNode.isObject() && !jsonNode.has(fieldName)) {
      ObjectNode objectNode = (ObjectNode) jsonNode;

      objectNode.putNull(fieldName);
    }
  }

  public static void defaultMissing(JsonNode jsonNode, String fieldName) {
    defaultMissing(jsonNode, fieldName, DEFAULT_MISSING_VALUE);
  }

  public static void defaultMissing(JsonNode jsonNode, String fieldName, String missingValue) {
    if (jsonNode.isObject() && !jsonNode.has(fieldName)) {
      ObjectNode objectNode = (ObjectNode) jsonNode;

      objectNode.put(fieldName, missingValue);
    }
  }

  public static boolean removeValue(ArrayNode arrayNode, String value) {
    for (int i = 0; i < arrayNode.size(); i++) {
      JsonNode element = arrayNode.get(i);

      if (element.isTextual()) {
        String textValue = element.textValue();
        if (textValue.equals(value)) {
          arrayNode.remove(i);

          return true;
        }
      }

      if (element.isNull() && value == null) {
        arrayNode.remove(i);

        return true;
      }
    }

    return false;
  }

  public static void addAll(ArrayNode arrayNode, Iterable<String> values) {
    for (String value : values) {
      arrayNode.add(value);
    }
  }

}
