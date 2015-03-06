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
package org.icgc.dcc.etl2.job.index.transform;

import static com.google.common.base.Objects.firstNonNull;
import static org.icgc.dcc.etl2.job.index.model.CollectionFieldAccessors.getDonorGeneId;
import static org.icgc.dcc.etl2.job.index.model.CollectionFieldAccessors.getDonorGenes;
import static org.icgc.dcc.etl2.job.index.model.CollectionFieldAccessors.getDonorId;
import static org.icgc.dcc.etl2.job.index.model.CollectionFieldAccessors.getObservationConsequenceGeneId;
import static org.icgc.dcc.etl2.job.index.model.CollectionFieldAccessors.getObservationConsequences;
import static org.icgc.dcc.etl2.job.index.model.CollectionFieldAccessors.getObservationType;
import static org.icgc.dcc.etl2.job.index.model.CollectionFieldAccessors.removeObservationConsequenceGeneId;
import static org.icgc.dcc.etl2.job.index.util.Fakes.FAKE_GENE_ID;
import static org.icgc.dcc.etl2.job.index.util.Fakes.isFakeGeneId;
import static org.icgc.dcc.etl2.job.index.util.JsonNodes.isEmpty;

import java.util.Map;

import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.etl2.job.index.core.Document;
import org.icgc.dcc.etl2.job.index.core.DocumentContext;
import org.icgc.dcc.etl2.job.index.core.DocumentTransform;
import org.icgc.dcc.etl2.job.index.util.Fakes;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;

/**
 * {@link DocumentTransform} implementation that creates a nested donor-centric document.
 */
public class DonorCentricDocumentTransform extends AbstractCentricDocumentTransform {

  private final DocumentTransform delegate = new DonorDocumentTransform();

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

      if (isEmpty(consequences)) {
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
