/*
 * Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.                             
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

import static org.icgc.dcc.release.core.util.Keys.getKey;
import static org.icgc.dcc.release.core.util.Tuples.tuple;

import java.util.Collection;

import lombok.val;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

public class KeyFieldsFunction<R> implements PairFunction<ObjectNode, String, R> {

  private final Collection<String> fieldNames;
  private final Function<ObjectNode, R> valueFunction;

  public KeyFieldsFunction(Function<ObjectNode, R> valueFunction, String... fieldNames) {
    this(valueFunction, ImmutableList.copyOf(fieldNames));
  }

  public KeyFieldsFunction(Function<ObjectNode, R> valueFunction, Collection<String> fieldNames) {
    this.fieldNames = fieldNames;
    this.valueFunction = valueFunction;
  }

  @Override
  public Tuple2<String, R> call(ObjectNode row) throws Exception {
    val key = getKey(row, fieldNames);

    return tuple(key, valueFunction.call(row));
  }

}
