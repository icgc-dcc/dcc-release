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
package org.icgc.dcc.release.job.document.transform;

import static com.google.common.base.Objects.firstNonNull;
import static org.icgc.dcc.release.core.util.ObjectNodes.isEmptyNode;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getDonorGeneId;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getDonorGenes;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getDonorId;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getObservationConsequenceGeneId;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getObservationConsequences;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getObservationType;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.removeObservationConsequenceGeneId;
import static org.icgc.dcc.release.job.document.util.Fakes.FAKE_GENE_ID;
import static org.icgc.dcc.release.job.document.util.Fakes.isFakeGeneId;
import static org.icgc.dcc.release.job.document.util.JsonNodes.isEmpty;

import java.util.Map;

import lombok.NonNull;
import lombok.val;

import org.apache.spark.api.java.function.Function;
import org.icgc.dcc.release.core.document.Document;
import org.icgc.dcc.release.job.document.context.DonorCentricDocumentContext;
import org.icgc.dcc.release.job.document.core.DocumentContext;
import org.icgc.dcc.release.job.document.core.DocumentTransform;
import org.icgc.dcc.release.job.document.core.DocumentJobContext;
import org.icgc.dcc.release.job.document.util.Fakes;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;

/**
 * {@link DocumentTransform} implementation that creates a nested donor-centric document.
 */
public class DonorCentricDocumentTransform extends AbstractCentricDocumentTransform implements
    Function<Tuple2<String, Tuple2<ObjectNode, Optional<Iterable<ObjectNode>>>>, Document> {

  @NonNull
  private final DocumentJobContext indexJobContext;
  private final DocumentTransform delegate;

  public DonorCentricDocumentTransform(DocumentJobContext indexJobContext) {
    this.indexJobContext = indexJobContext;
    this.delegate = new DonorDocumentTransform(indexJobContext);
  }

  @Override
  public Document call(Tuple2<String, Tuple2<ObjectNode, Optional<Iterable<ObjectNode>>>> tuple) throws Exception {
    val donor = tuple._2._1;
    val donorId = getDonorId(donor);
    val observations = tuple._2._2;
    val documentContext = new DonorCentricDocumentContext(donorId, indexJobContext, observations);

    return transformDocument(donor, documentContext);
  }

  @Override
  public Document transformDocument(@NonNull ObjectNode donor, @NonNull DocumentContext context) {
    // Delegate for reuse (embed project under donor)
    delegate.transformDocument(donor, context);

    // Index observations by gene id
    val donorId = getDonorId(donor);
    val donorObservations = context.getObservationsByDonorId(donorId);
    val donorGenes = getDonorGenes(donor);
    val donorGenesObservations = indexDonorGenesObservations(donorObservations);
    val donorGeneIds = donorGenesObservations.keySet();
    val donorGeneSummaries = indexDonorGeneSummaries(donorGenes);

    // Nest
    for (val donorGeneId : donorGeneIds) {
      val gene = context.getGene(donorGeneId).deepCopy();

      // Construct
      val donorGeneObservations = donorGenesObservations.get(donorGeneId);
      val donorGeneTree = createDonorGeneTree(gene.deepCopy(), donorGeneObservations);

      // Merge
      val donorGene = donorGeneSummaries.get(donorGeneId);
      donorGene.putAll(donorGeneTree);
    }

    if (isEmpty(donorGenes)) {
      // Ensure arrays are present with at least 1 element
      donorGenes.add(Fakes.createPlaceholder());
    }

    return new Document(context.getType(), donorId, donor);
  }

  private static ObjectNode createDonorGeneTree(ObjectNode donorGene, Iterable<ObjectNode> donorGeneObservations) {
    // Process each observation associated with the current gene
    for (ObjectNode donorGeneObservation : donorGeneObservations) {
      // Localize changes
      donorGeneObservation = donorGeneObservation.deepCopy();

      // Add to feature type array (e.g. "ssm")
      val observationType = getObservationType(donorGeneObservation);
      val array = donorGene.withArray(observationType);

      // Remove unrelated gene consequences
      transformGeneObservationConsequences(donorGene, donorGeneObservation);

      // Remove obsolete fields
      for (val consequence : getObservationConsequences(donorGeneObservation)) {
        removeObservationConsequenceGeneId(consequence);
      }

      array.add(donorGeneObservation);
    }

    return donorGene;
  }

  private static Map<String, ObjectNode> indexDonorGeneSummaries(ArrayNode donorGenes) {
    val index = ImmutableMap.<String, ObjectNode> builder();

    boolean hasFake = false;
    for (val value : donorGenes) {
      val donorGene = (ObjectNode) value;
      val donorGeneId = firstNonNull(getDonorGeneId(donorGene), FAKE_GENE_ID);
      if (isFakeGeneId(donorGeneId)) {
        hasFake = true;
      }

      index.put(donorGeneId, donorGene);
    }

    if (!hasFake) {
      // Special case since the summarize did not produce
      val fake = Fakes.createFakeGene();
      donorGenes.add(fake);

      index.put(FAKE_GENE_ID, fake);
    }

    return index.build();
  }

  private static Multimap<String, ObjectNode> indexDonorGenesObservations(Iterable<ObjectNode> observations) {
    // Set and multi-semantics are required
    val index = ImmutableSetMultimap.<String, ObjectNode> builder();

    for (val observation : observations) {
      val consequences = getObservationConsequences(observation);

      if (isEmptyNode(consequences)) {
        index.put(FAKE_GENE_ID, observation);
      } else {
        for (val consequence : consequences) {
          index.put(resolveObservationConsequenceGeneId(consequence), observation);
        }
      }
    }

    return index.build();
  }

  private static String resolveObservationConsequenceGeneId(JsonNode consequence) {
    val geneId = getObservationConsequenceGeneId(consequence);

    return firstNonNull(geneId, FAKE_GENE_ID);
  }

}
