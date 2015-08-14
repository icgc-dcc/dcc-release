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
package org.icgc.dcc.etl2.core.function;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.disjoint;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.etl2.core.util.Tuples.tuple;

import java.util.Collections;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.icgc.dcc.common.core.util.Joiners;
import org.icgc.dcc.common.core.util.Splitters;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

@RequiredArgsConstructor(access = PRIVATE)
public class UnwindToPair<KF, VF> implements PairFlatMapFunction<ObjectNode, KF, VF> {

  @NonNull
  private final String unwindField;
  @NonNull
  private final Function<ObjectNode, KF> keyFunction;
  @NonNull
  private final Function<ObjectNode, VF> valueFunction;
  private final boolean includeParent;

  /**
   * Returns function which unwinds {@code unwindField}. After the object created applies {@code keyFunction} to the key
   * and {@code valueFunction} to the value.<br>
   * <br>
   * The function returns an object that contains nested object's fields or empty collection if array at the unwind path
   * does not exist
   */
  @NonNull
  public static <KF, VF> UnwindToPair<KF, VF> unwind(String unwindField, Function<ObjectNode, KF> keyFunction,
      Function<ObjectNode, VF> valueFunction) {
    return new UnwindToPair<>(unwindField, keyFunction, valueFunction, false);
  }

  /**
   * Returns function which unwinds {@code unwindField} and joins it to the parent object. After the object created
   * applies {@code keyFunction} to the key and {@code valueFunction} to the value.<br>
   * <br>
   * The function returns an object that contains parent's and nested object's fields or empty collection if array at
   * the unwind path does not exist
   */
  @NonNull
  public static <KF, VF> UnwindToPair<KF, VF> unwindToParent(String unwindField, Function<ObjectNode, KF> keyFunction,
      Function<ObjectNode, VF> valueFunction) {
    return new UnwindToPair<>(unwindField, keyFunction, valueFunction, true);
  }

  @Override
  public Iterable<Tuple2<KF, VF>> call(ObjectNode row) throws Exception {
    val elements = getUnwindArray(row);
    if (elements.isMissingNode()) {
      return Collections.emptyList();
    }
    checkArgument(elements.isArray(), "'%s' is not an array on object %s", unwindField, row);

    val result = ImmutableList.<Tuple2<KF, VF>> builder();
    for (val element : elements) {
      val unwinded = createUnwindedObject(row, (ObjectNode) element);
      val key = keyFunction.call(unwinded);
      val value = valueFunction.call(unwinded);
      result.add(tuple(key, value));
    }

    return result.build();
  }

  private ObjectNode createUnwindedObject(ObjectNode row, ObjectNode element) {
    ObjectNode resultObject = element;

    if (includeParent) {
      resultObject = row.deepCopy();
      checkArgument(!hasDuplicateFields(row, element), "Failed to unwind element[%s]. "
          + "Parent object contains duplicate fields. Parent object fields: %s",
          joinFields(element),
          joinFields(row));
      resultObject.remove(unwindField);
      resultObject.setAll(element);
    }

    return resultObject;
  }

  private static String joinFields(ObjectNode json) {
    return Joiners.COMMA.join(json.fieldNames());
  }

  private static boolean hasDuplicateFields(ObjectNode parent, ObjectNode element) {
    val parentFields = ImmutableSet.copyOf(parent.fieldNames());
    val childFields = ImmutableSet.copyOf(element.fieldNames());

    return !disjoint(parentFields, childFields);
  }

  private JsonNode getUnwindArray(ObjectNode row) {
    JsonNode result = null;
    for (val fieldName : Splitters.DOT.split(unwindField)) {
      if (result == null) {
        result = row.path(fieldName);
      } else {
        result = result.path(fieldName);
      }

      if (result.isMissingNode()) {
        return result;
      }
    }

    return result;
  }

}