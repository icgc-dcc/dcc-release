/*
 * Copyright (c) 2014 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.release.job.document.transform;

import static com.google.common.base.Objects.firstNonNull;
import static org.icgc.dcc.common.core.model.FieldNames.CONSEQUENCE_AA_MUTATION;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_SYMBOL;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_CHROMOSOME;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_CHROMOSOME_START;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_ID;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getMutationId;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getObservationConsequenceGeneId;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getObservationConsequences;
import static org.icgc.dcc.release.job.document.util.Fakes.FAKE_GENE_ID;
import static org.icgc.dcc.release.job.document.util.Fakes.createFakeGene;
import static org.icgc.dcc.release.job.document.util.Fakes.isFakeGeneId;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.spark.api.java.function.Function;
import org.icgc.dcc.common.core.model.FieldNames.NormalizerFieldNames;
import org.icgc.dcc.release.core.document.Document;
import org.icgc.dcc.release.job.document.context.MutationCentricDocumentContext;
import org.icgc.dcc.release.job.document.core.DocumentContext;
import org.icgc.dcc.release.job.document.core.DocumentJobContext;
import org.icgc.dcc.release.job.document.core.DocumentTransform;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;

/**
 * {@link DocumentTransform} implementation that creates a mutation document.
 */
@RequiredArgsConstructor
public class MutationTextDocumentTransform implements DocumentTransform,
    Function<Tuple2<String, Tuple2<ObjectNode, Optional<Iterable<ObjectNode>>>>, Document> {

  @NonNull
  private final DocumentJobContext documentJobContext;

  @Override
  public Document call(Tuple2<String, Tuple2<ObjectNode, Optional<Iterable<ObjectNode>>>> tuple) throws Exception {
    val mutation = tuple._2._1;
    val observations = tuple._2._2;
    val mutationId = getMutationId(mutation);
    val documentContext = new MutationCentricDocumentContext(mutationId, documentJobContext, observations);

    return transformDocument(mutation, documentContext);
  }

  @Override
  public Document transformDocument(@NonNull ObjectNode mutation, @NonNull DocumentContext context) {
    // Get observations of this mutation
    val mutationId = getMutationId(mutation);
    val observations = context.getObservationsByMutationId(mutationId);

    // - f(transcriptId) -> consequence
    val geneMutations = Sets.<String> newHashSet();

    // For each mutation observation in turn:
    for (val observation : observations) {
      // For each consequence of the current mutation observation:
      val consequences = getObservationConsequences(observation);

      for (val consequence : consequences) {
        // Get gene associated with consequence: {@code consequence._gene_id}
        val geneId = firstNonNull(getObservationConsequenceGeneId(consequence), FAKE_GENE_ID);
        val gene = isFakeGeneId(geneId) ? createFakeGene() : context.getGene(geneId);
        val geneSymbol = gene.path(GENE_SYMBOL);
        val aaMutation = consequence.path(CONSEQUENCE_AA_MUTATION);

        // Index transcript by transcript id
        if (hasValue(geneSymbol) && hasValue(aaMutation)) {
          geneMutations.add(geneSymbol.asText() + " " + aaMutation.asText());
        }
      }
    }

    mutation.put("id", mutationId);

    mutation.putPOJO("geneMutations", geneMutations);

    mutation.put(
        "mutation",
        formatMutationString(
            mutation.path(MUTATION_CHROMOSOME).asText(),
            mutation.path(MUTATION_CHROMOSOME_START).asText(),
            mutation.path(NormalizerFieldNames.NORMALIZER_MUTATION).asText()));
    mutation.put("type", "mutation");
    mutation.put("start", mutation.path(MUTATION_CHROMOSOME_START).asText());

    mutation.remove(MUTATION_ID);
    mutation.remove(MUTATION_CHROMOSOME);
    mutation.remove(MUTATION_CHROMOSOME_START);

    return new Document(context.getType(), mutationId, mutation);
  }

  private String formatMutationString(String chromosome, String chromosomeStart, String mutation) {
    return "chr" + chromosome + ":g." + chromosomeStart + mutation;
  }

  private static boolean hasValue(JsonNode node) {
    return !node.isNull() && node.isValueNode();
  }

}
