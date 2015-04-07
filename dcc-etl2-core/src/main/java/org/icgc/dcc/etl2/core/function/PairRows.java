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

import lombok.val;

import org.apache.spark.api.java.function.PairFunction;
import org.icgc.dcc.etl2.core.util.Keys;

import scala.Tuple2;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;

public class PairRows implements PairFunction<Tuple2<String, Tuple2<ObjectNode, Optional<Iterable<ObjectNode>>>>,
    String, Tuple2<ObjectNode, Optional<Iterable<ObjectNode>>>> {

  private final String[] fieldNames;

  public PairRows(String... fieldNames) {
    this.fieldNames = fieldNames;
  }

  @Override
  public Tuple2<String, Tuple2<ObjectNode, Optional<Iterable<ObjectNode>>>> call(
      Tuple2<String, Tuple2<ObjectNode, Optional<Iterable<ObjectNode>>>> tuple) {
    val association = tuple._2;
    val left = association._1;
    val key = Keys.getKey(left, fieldNames);

    return pair(key, association);
  }

  private Tuple2<String, Tuple2<ObjectNode, Optional<Iterable<ObjectNode>>>> pair(String donorId,
      Tuple2<ObjectNode, Optional<Iterable<ObjectNode>>> specimenSample) {

    return new Tuple2<String, Tuple2<ObjectNode, Optional<Iterable<ObjectNode>>>>(donorId, specimenSample);
  }

}