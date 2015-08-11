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
package org.icgc.dcc.etl2.job.join.function;

import static org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames.CONSEQUENCE_ARRAY_NAME;
import static org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames.OBSERVATION_ARRAY_NAME;
import lombok.val;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;

public class CombineObservation
    implements
    Function<Tuple2<String, Tuple2<ObjectNode, Optional<Iterable<Tuple2<ObjectNode, Optional<Iterable<ObjectNode>>>>>>>, ObjectNode> {

  @Override
  public ObjectNode call(
      Tuple2<String, Tuple2<ObjectNode, Optional<Iterable<Tuple2<ObjectNode, Optional<Iterable<ObjectNode>>>>>>> tuple)
      throws Exception {
    val meta = tuple._2._1;
    val observations = meta.withArray(OBSERVATION_ARRAY_NAME);

    addObservations(observations, tuple._2._2);

    return meta;
  }

  private void addObservations(ArrayNode observations,
      Optional<Iterable<Tuple2<ObjectNode, Optional<Iterable<ObjectNode>>>>> value) {
    if (!value.isPresent()) {
      return;
    }

    for (val tuple : value.get()) {
      val observation = tuple._1;
      val consequences = observation.withArray(CONSEQUENCE_ARRAY_NAME);

      addConsequences(consequences, tuple._2);

      observations.add(observation);
    }
  }

  private void addConsequences(ArrayNode consequences, Optional<Iterable<ObjectNode>> value) {
    if (!value.isPresent()) {
      return;
    }

    for (val consequence : value.get()) {
      consequences.add(consequence);
    }
  }

}