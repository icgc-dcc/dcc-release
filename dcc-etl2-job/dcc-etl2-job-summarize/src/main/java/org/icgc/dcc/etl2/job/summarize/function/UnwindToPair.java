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
package org.icgc.dcc.etl2.job.summarize.function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.copyOf;
import static org.icgc.dcc.etl2.core.util.Tuples.tuple;

import java.util.Collections;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.icgc.dcc.common.core.util.Joiners;

import scala.Tuple2;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

@RequiredArgsConstructor
public class UnwindToPair<KF, VF> implements PairFlatMapFunction<ObjectNode, KF, VF> {

  @NonNull
  private final String unwindField;
  @NonNull
  private final Function<ObjectNode, KF> keyFunction;
  @NonNull
  private final Function<ObjectNode, VF> valueFunction;

  @Override
  public Iterable<Tuple2<KF, VF>> call(ObjectNode row) throws Exception {
    val elements = row.get(unwindField);
    if (elements == null) {
      return Collections.emptyList();
    }

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
    checkArgument(!hasDuplicateFields(row, element), "Failed to unwind element[%s]. "
        + "Parent object contains duplicate fields. Parent object fields: %s",
        joinFields(element),
        joinFields(row));

    val resultObject = row.deepCopy();
    resultObject.remove(unwindField);
    resultObject.setAll(element);

    return resultObject;
  }

  private static String joinFields(ObjectNode json) {
    return Joiners.COMMA.join(json.fieldNames());
  }

  private static boolean hasDuplicateFields(ObjectNode parent, ObjectNode element) {
    val parentFields = copyOf(parent.fieldNames());
    val childFields = copyOf(element.fieldNames());

    return !Sets.intersection(parentFields, childFields).isEmpty();
  }

}