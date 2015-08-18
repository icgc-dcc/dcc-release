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

import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static org.icgc.dcc.common.core.model.FeatureTypes.FeatureType.SSM_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_GENES;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_GENE_GENE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_GENE_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SUMMARY_AFFECTED_GENE_COUNT;
import static org.icgc.dcc.common.core.util.Jackson.DEFAULT;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;
import static org.icgc.dcc.etl2.core.util.ObjectNodes.textValue;
import static org.icgc.dcc.etl2.core.util.Tuples.tuple;

import java.util.Map.Entry;
import java.util.function.Consumer;

import lombok.val;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class CreateDonorGenesSummary implements
    PairFunction<Tuple2<String, Iterable<ObjectNode>>, String, ObjectNode> {

  @Override
  public Tuple2<String, ObjectNode> call(Tuple2<String, Iterable<ObjectNode>> tuple) throws Exception {
    val donorSummary = DEFAULT.createObjectNode();
    val genes = donorSummary.withArray(DONOR_GENES);

    val geneIds = tuple._2;
    val geneIdCounts = stream(geneIds)
        .map(gene -> textValue(gene, DONOR_GENE_GENE_ID))
        .collect(groupingBy(geneId -> geneId, counting()));

    geneIdCounts.entrySet().stream()
        .forEach(createAndPutSummaryGene(genes));

    val summary = donorSummary.with(DONOR_GENE_SUMMARY);
    summary.put(DONOR_SUMMARY_AFFECTED_GENE_COUNT, geneIdCounts.size());

    val donorId = tuple._1;
    return tuple(donorId, donorSummary);
  }

  private Consumer<? super Entry<String, Long>> createAndPutSummaryGene(ArrayNode genes) {
    return e -> {
      ObjectNode result = DEFAULT.createObjectNode();
      result.put(DONOR_GENE_GENE_ID, e.getKey());
      ObjectNode summary = DEFAULT.createObjectNode();
      result.put(DONOR_GENE_SUMMARY, summary);

      summary.put(SSM_TYPE.getSummaryFieldName(), e.getValue());
      genes.add(result);
    };
  }

}