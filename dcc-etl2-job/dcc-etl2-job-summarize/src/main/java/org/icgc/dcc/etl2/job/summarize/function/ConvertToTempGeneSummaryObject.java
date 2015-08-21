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

import static org.icgc.dcc.common.core.model.FieldNames.DONOR_ID;
import static org.icgc.dcc.common.core.model.FieldNames.PROJECT_ID;
import static org.icgc.dcc.etl2.core.util.Keys.KEY_SEPARATOR;
import static org.icgc.dcc.etl2.core.util.ObjectNodes.createObject;
import static org.icgc.dcc.etl2.core.util.Tuples.tuple;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.fasterxml.jackson.databind.node.ObjectNode;

@RequiredArgsConstructor
public final class ConvertToTempGeneSummaryObject implements PairFunction<Tuple2<String, Integer>, String, ObjectNode> {

  private static final int DONOR_ID_INDEX = 0;
  private static final int TYPE_INDEX = 1;
  private static final int GENE_ID_INDEX = 2;

  @NonNull
  private final String projectName;

  @Override
  public Tuple2<String, ObjectNode> call(Tuple2<String, Integer> tuple) throws Exception {
    val donorTypeGene = tuple._1;
    val count = tuple._2;
    val donorId = getDonorId(donorTypeGene);
    val type = getType(donorTypeGene);
    val resultObject = createObject();
    resultObject.put(DONOR_ID, donorId);
    resultObject.put(type, count);
    resultObject.put(PROJECT_ID, projectName);
    val geneId = getGeneId(donorTypeGene);

    return tuple(geneId, resultObject);
  }

  private static String getDonorId(String donorTypeGene) {
    return donorTypeGene.split(KEY_SEPARATOR)[DONOR_ID_INDEX];
  }

  private static String getType(String donorTypeGene) {
    return donorTypeGene.split(KEY_SEPARATOR)[TYPE_INDEX];
  }

  private static String getGeneId(String donorTypeGene) {
    return donorTypeGene.split(KEY_SEPARATOR)[GENE_ID_INDEX];
  }

}