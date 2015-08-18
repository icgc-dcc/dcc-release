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
package org.icgc.dcc.etl2.core.util;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static org.icgc.dcc.common.core.util.Jackson.asObjectNode;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.NonNull;
import lombok.val;
import lombok.experimental.UtilityClass;

import org.icgc.dcc.common.core.util.Splitters;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

@UtilityClass
public class ObjectNodes {

  public static final ObjectMapper MAPPER = new ObjectMapper();

  private static final Splitter PATH_SPLITTER = Splitters.DOT;
  private static final JsonNodeFactory JSON_FACTORY = new JsonNodeFactory(false);

  /**
   * Returns value for {@code fieldName} as String.
   * @return value or null if {@code jsonNode} is {@code null} or {@code fieldName} is missing.
   */
  public static String textValue(JsonNode jsonNode, @NonNull String fieldName) {
    if (jsonNode == null) {
      return null;
    }

    val fieldValue = jsonNode.path(fieldName);

    return fieldValue.isMissingNode() || fieldValue.isNull() ? null : fieldValue.asText();
  }

  public static JsonNode getPath(@NonNull ObjectNode objectNode, @NonNull String path) {
    val parts = parsePath(path);

    JsonNode jsonNode = objectNode;
    for (val fieldName : parts) {
      jsonNode = jsonNode.get(fieldName);
      if (jsonNode == null) {
        // Missing
        return null;
      }
    }

    return jsonNode;
  }

  @NonNull
  public static boolean isEmpty(ArrayNode array) {
    return ImmutableList.copyOf(array).isEmpty();
  }

  public static boolean contains(@NonNull ArrayNode array, JsonNode element) {
    return stream(array)
        .anyMatch(e -> e.equals(element));
  }

  @NonNull
  public static ObjectNode mergeObjects(ObjectNode targetNode, ObjectNode sourceNode) {
    val result = targetNode.deepCopy();
    val fieldNames = sourceNode.fieldNames();

    while (fieldNames.hasNext()) {
      val fieldName = fieldNames.next();
      val sourceValue = sourceNode.get(fieldName);

      if (sourceValue.isObject()) {
        val targetObject = result.path(fieldName);
        val targetValue = targetObject.isMissingNode() ?
            sourceValue :
            mergeObjects(asObjectNode(targetObject), asObjectNode(sourceValue));
        result.put(fieldName, targetValue);
        continue;
      }

      checkArgument(result.path(fieldName).isMissingNode(), "Found duplicate field name '%s' in parent object %s",
          fieldName, result);
      result.put(fieldName, sourceNode.get(fieldName));
    }

    return result;
  }

  @NonNull
  public static <T> Collection<JsonNode> createArrayValues(Iterable<T> values) {
    val result = ImmutableList.<JsonNode> builder();

    for (val value : values) {
      if (value instanceof String) {
        result.add(JSON_FACTORY.textNode(String.valueOf(value)));
      } else if (isNumber(value)) {
        result.add(createNumberNode(value));
      } else if (value instanceof Boolean) {
        result.add(createBooleanNode((Boolean) value));
      }
    }

    return result.build();
  }

  @NonNull
  public static BooleanNode createBooleanNode(Boolean value) {
    return JSON_FACTORY.booleanNode(value);
  }

  public static <T> JsonNode createNumberNode(T value) {
    if (value instanceof Integer) {
      return JSON_FACTORY.numberNode((Integer) value);
    } else if (value instanceof Long) {
      return JSON_FACTORY.numberNode((Long) value);
    } else if (value instanceof Double) {
      return JSON_FACTORY.numberNode((Double) value);
    } else if (value instanceof Float) {
      return JSON_FACTORY.numberNode((Float) value);
    }

    throw new IllegalArgumentException(format("Failed to create a Json number node from %s", value));
  }

  private static <T> boolean isNumber(final T value) {
    return value instanceof Integer;
  }

  private static Iterable<String> parsePath(String path) {
    val parts = PATH_SPLITTER.split(path);
    return parts;
  }

  @NonNull
  public static String toEmptyJsonValue(Set<String> fields) {
    String joined = fields.stream()
        .map(i -> "\"" + i.toString() + "\":\"\"")
        .collect(Collectors.joining(", "));

    return "[" + joined + "]";
  }

}
