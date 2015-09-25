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
package org.icgc.dcc.release.core.function;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.disjoint;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toList;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.util.Jackson.asArrayNode;
import static org.icgc.dcc.common.core.util.Jackson.asObjectNode;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;
import static org.icgc.dcc.release.core.util.Collections.isLast;
import static org.icgc.dcc.release.core.util.ObjectNodes.MAPPER;
import static org.icgc.dcc.release.core.util.ObjectNodes.isEmptyArray;

import java.util.List;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.icgc.dcc.common.core.util.Joiners;
import org.icgc.dcc.common.core.util.Splitters;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * A function which unwinds {@code unwindPath} and joins it to the parent object if requested.<br>
 * <br>
 * The function works like <a href="http://docs.mongodb.org/manual/reference/operator/aggregation/unwind/">Mongo's
 * unwind function</a>, however if it was requested to include the parent object and requested to unwind array does not
 * exists only parent's fields will be returned.
 */
@RequiredArgsConstructor(access = PRIVATE)
public class Unwind implements FlatMapFunction<ObjectNode, ObjectNode> {

  @NonNull
  private final String unwindPath;
  private final boolean includeParent;

  public static Unwind unwind(@NonNull String unwindPath) {
    return new Unwind(unwindPath, false);
  }

  public static Unwind unwindToParent(@NonNull String unwindPath) {
    return new Unwind(unwindPath, true);
  }

  @Override
  public Iterable<ObjectNode> call(@NonNull ObjectNode row) throws Exception {
    val elements = unwindPath(unwindPath, row);
    if (isMissingElements(elements)) {
      return createMissingElementsObject(row);
    }
    checkArgument(elements.isArray(), "'%s' is not an array on object %s", unwindPath, row);

    return stream(elements)
        .map(e -> createResultObject(row, asObjectNode(e)))
        .collect(toList());
  }

  private Iterable<ObjectNode> createMissingElementsObject(ObjectNode row) {
    if (includeParent) {
      row.remove(resolveTopLevelFieldName(unwindPath));

      return singleton(row);
    }

    return emptyList();
  }

  private boolean isMissingElements(JsonNode elements) {
    return elements.isMissingNode() || (elements.isArray() && isEmptyArray(asArrayNode(elements)));
  }

  private static JsonNode unwindPath(String path, ObjectNode row) {
    val fieldNames = ImmutableList.copyOf(Splitters.DOT.split(path));

    return getUnwindArray(row, fieldNames);
  }

  private static JsonNode getUnwindArray(ObjectNode row, List<String> fieldNames) {
    val result = MAPPER.createArrayNode();
    JsonNode currentNode = null;
    for (val fieldName : fieldNames) {
      currentNode = currentNode == null ? row.path(fieldName) : currentNode.path(fieldName);
      if (currentNode.isMissingNode()) {
        return isEmptyArray(result) ? currentNode : result;
      }

      if (currentNode.isArray()) {
        if (isLast(fieldNames, fieldName)) {
          result.addAll(asArrayNode(currentNode));
        } else {
          result.addAll(unwindArray(asArrayNode(currentNode), getLeftFieldNames(fieldNames, fieldName)));
        }
      }
      else if (isLast(fieldNames, fieldName)) {
        throw new IllegalArgumentException("Failed to unwind non-array field. Reached the final level field. The field"
            + " exists but it's not an array one.");
      }
    }

    return result;
  }

  private static List<String> getLeftFieldNames(List<String> fieldNames, String fieldName) {
    val index = fieldNames.indexOf(fieldName);

    return fieldNames.subList(index + 1, fieldNames.size());
  }

  private static ArrayNode unwindArray(ArrayNode sourceArray, List<String> fieldNames) {
    val result = MAPPER.createArrayNode();
    for (val element : sourceArray) {
      val resultArray = getUnwindArray(asObjectNode(element), fieldNames);
      if (resultArray.isArray()) {
        result.addAll(asArrayNode(resultArray));
      }
    }

    return result;
  }

  private ObjectNode createResultObject(ObjectNode row, ObjectNode element) {
    ObjectNode resultObject = element;
    if (includeParent) {
      resultObject = row.deepCopy();
      checkDuplicateFields(row, element);
      resultObject.remove(resolveTopLevelFieldName(unwindPath));
      resultObject.setAll(element);
    }

    return resultObject;
  }

  private static void checkDuplicateFields(ObjectNode row, ObjectNode element) {
    checkArgument(!hasDuplicateFields(row, element), "Failed to unwind element[%s]. "
        + "Parent object contains duplicate fields. Parent object fields: %s",
        joinFields(element),
        joinFields(row));
  }

  private static String resolveTopLevelFieldName(String unwindPath) {
    return Splitters.DOT.split(unwindPath).iterator().next();
  }

  private static String joinFields(ObjectNode json) {
    return Joiners.COMMA.join(json.fieldNames());
  }

  private static boolean hasDuplicateFields(ObjectNode parent, ObjectNode element) {
    val parentFields = ImmutableSet.copyOf(parent.fieldNames());
    val childFields = ImmutableSet.copyOf(element.fieldNames());

    return !disjoint(parentFields, childFields);
  }

}