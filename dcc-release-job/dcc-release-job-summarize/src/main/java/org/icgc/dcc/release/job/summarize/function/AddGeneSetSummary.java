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

import static org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames.SUMMARY;
import static org.icgc.dcc.release.core.util.FieldNames.SummarizeFieldNames.GENE_COUNT;
import lombok.val;

import org.apache.spark.api.java.function.Function;
import org.icgc.dcc.common.json.Jackson;

import scala.Tuple2;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;

public final class AddGeneSetSummary implements
    Function<Tuple2<String, Tuple2<ObjectNode, Optional<Integer>>>, ObjectNode> {

  @Override
  public ObjectNode call(Tuple2<String, Tuple2<ObjectNode, Optional<Integer>>> tuple) throws Exception {
    val geneSet = tuple._2._1;
    val count = tuple._2._2;
    if (count.isPresent()) {
      val summary = createSummary(count.get());
      geneSet.put(SUMMARY, summary);
    }

    return geneSet;
  }

  private static ObjectNode createSummary(Integer count) {
    val summary = Jackson.DEFAULT.createObjectNode();
    summary.put(GENE_COUNT, count);

    return summary;
  }

}