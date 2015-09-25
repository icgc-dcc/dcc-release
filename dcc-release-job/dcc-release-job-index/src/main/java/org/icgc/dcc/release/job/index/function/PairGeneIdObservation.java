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
package org.icgc.dcc.release.job.index.function;

import static org.icgc.dcc.release.core.util.Tuples.tuple;
import static org.icgc.dcc.release.job.index.model.CollectionFieldAccessors.getObservationConsequenceGeneIds;

import java.util.List;

import lombok.val;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public final class PairGeneIdObservation implements PairFlatMapFunction<ObjectNode, String, ObjectNode> {

  @Override
  public Iterable<Tuple2<String, ObjectNode>> call(ObjectNode observation) throws Exception {
    val uniqueGeneIds = Sets.newHashSet(getObservationConsequenceGeneIds(observation));
    List<Tuple2<String, ObjectNode>> values = Lists.newArrayListWithCapacity(uniqueGeneIds.size());
    for (val observationGeneId : uniqueGeneIds) {
      if (observationGeneId != null) {
        values.add(tuple(observationGeneId, observation));
      }
    }

    return values;
  }

}