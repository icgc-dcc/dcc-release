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
package org.icgc.dcc.release.job.summarize.function;

import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SUMMARY;
import static org.icgc.dcc.release.core.util.FeatureTypes.createFeatureTypeSummaryValue;
import static org.icgc.dcc.release.core.util.ObjectNodes.MAPPER;
import static org.icgc.dcc.release.core.util.Tuples.tuple;
import lombok.val;

import org.apache.spark.api.java.function.PairFunction;
import org.icgc.dcc.common.core.model.FeatureTypes.FeatureType;
import org.icgc.dcc.common.core.util.Splitters;

import scala.Tuple2;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Iterables;

public final class CreateFeatureTypeSummary implements PairFunction<Tuple2<String, Integer>, String, ObjectNode> {

  @Override
  public Tuple2<String, ObjectNode> call(Tuple2<String, Integer> tuple) throws Exception {
    val donorFeatureType = tuple._1;
    val featureType = resolveFeatureType(resolveTypeName(donorFeatureType));
    val donorSummary = MAPPER.createObjectNode();
    val summary = donorSummary.with(DONOR_SUMMARY);
    val featureTypeCount = tuple._2;
    val summaryValue = createFeatureTypeSummaryValue(featureType, featureTypeCount);
    summary.put(featureType.getSummaryFieldName(), summaryValue);

    return tuple(resolveDonorId(donorFeatureType), donorSummary);
  }

  private static FeatureType resolveFeatureType(String typeName) {
    return FeatureType.from(typeName);
  }

  private static String resolveTypeName(String key) {
    val parts = Splitters.HASHTAG.split(key);

    return Iterables.get(parts, 1);
  }

  private static String resolveDonorId(String key) {
    val parts = Splitters.HASHTAG.split(key);

    return parts.iterator().next();
  }

}