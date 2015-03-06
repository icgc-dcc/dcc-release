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

import lombok.val;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class TransformDonorMutationSsmPrimarySecondary implements
    Function<Tuple2<String, Iterable<Tuple2<String, Tuple2<ObjectNode, Iterable<ObjectNode>>>>>, ObjectNode> {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public ObjectNode call(
      Tuple2<String, Iterable<Tuple2<String, Tuple2<ObjectNode, Iterable<ObjectNode>>>>> tuple) throws Exception {

    // TODO: Factor primary in to primary1 and nested primary2 and nest secondary under primary1
    val donorMutations = tuple._2;

    ObjectNode mutation = null;
    ArrayNode consequences = null;
    ArrayNode observations = createObservations();

    for (val donorMutation : donorMutations) {
      val primary = donorMutation._2._1;
      if (mutation == null) {
        mutation = trimMutation(primary.deepCopy());
      }

      val observation = trimObservation(primary);
      observations.add(observation);

      if (consequences == null) {
        consequences = createConsequences();

        val secondaries = donorMutation._2._2;
        for (val secondary : secondaries) {
          consequences.add(secondary);
        }
      }
    }

    mutation.put("consequence", consequences);
    mutation.put("observation", observations);

    return mutation;
  }

  private ObjectNode trimMutation(ObjectNode mutation) {
    // TODO: Remove fields

    return mutation;
  }

  private ObjectNode trimObservation(ObjectNode observation) {
    // TODO: Remove fields

    return observation;
  }

  private ArrayNode createConsequences() {
    return MAPPER.createArrayNode();
  }

  private ArrayNode createObservations() {
    return MAPPER.createArrayNode();
  }

}