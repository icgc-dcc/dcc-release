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
import static org.icgc.dcc.common.core.model.FieldNames.GENE_ID;
import static org.icgc.dcc.release.job.document.util.Fakes.FAKE_GENE_ID;
import static org.icgc.dcc.release.job.document.util.Fakes.isFakeGeneId;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;

import org.apache.spark.api.java.function.Function;
import org.icgc.dcc.release.core.util.JacksonFactory;
import org.icgc.dcc.release.job.document.context.DonorCentricDocumentContext;
import org.icgc.dcc.release.job.document.core.DocumentContext;
import org.icgc.dcc.release.job.document.core.DocumentJobContext;
import org.icgc.dcc.release.job.document.core.DocumentTransform;
import org.icgc.dcc.release.job.document.model.Donor;
import org.icgc.dcc.release.job.document.model.Occurrence;
import org.icgc.dcc.release.job.document.model.Occurrence.Consequence;

import scala.Tuple2;

import com.esotericsoftware.kryo.Kryo;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

public final class DonorCentricDocumentTransform implements
    Function<Tuple2<String, Tuple2<ObjectNode, Optional<Collection<Occurrence>>>>, Donor> {

  private final DocumentJobContext documentJobContext;
  private final DocumentTransform delegate;
  private transient Kryo objectClonner;

  public DonorCentricDocumentTransform(@NonNull DocumentJobContext documentJobContext) {
    this.documentJobContext = documentJobContext;
    this.delegate = new DonorDocumentTransform(documentJobContext);

  }

  @Override
  public Donor call(Tuple2<String, Tuple2<ObjectNode, Optional<Collection<Occurrence>>>> tuple) throws Exception {
    val donorId = tuple._1;
    val donorJson = tuple._2._1;
    val documentContext = createContext(donorId);
    delegate.transformDocument(donorJson, documentContext);
    val donor = convertDonor(donorJson);

    val occurrencesOpt = tuple._2._2;
    if (!occurrencesOpt.isPresent()) {
      return donor;
    }

    checkObjectClonnerInitialized();
    val occurrences = occurrencesOpt.get();
    val donorGenesObservations = documentDonorGenesObservations(occurrences);
    val donorGeneSummaries = documentDonorGeneSummaries(donor.getGene());

    // Nest
    for (val donorGeneId : donorGenesObservations.keySet()) {
      val gene = getGene(donorGeneId);

      // Construct
      val donorGeneObservations = donorGenesObservations.get(donorGeneId);
      val donorGeneTree = createDonorGeneTree(gene, donorGeneObservations, objectClonner);
      //
      // // Merge
      val donorGene = donorGeneSummaries.get(donorGeneId);
      donorGene.putAll(donorGeneTree);
    }

    return donor;
  }

  @SneakyThrows
  private static Map<String, Object> createDonorGeneTree(Map<String, Object> donorGene,
      Iterable<Occurrence> donorGeneObservations, Kryo objectClonner) {
    val gene = Maps.newHashMap(donorGene);
    // Process each observation associated with the current gene
    for (val donorGeneObservationOriginal : donorGeneObservations) {
      val donorGeneObservation = objectClonner.copy(donorGeneObservationOriginal);

      // Add to feature type array (e.g. "ssm")
      val observationType = donorGeneObservation.get_type();
      val array = getTypeArray(gene, observationType);

      // Remove unrelated gene consequences
      val consequences = filterGeneObservationConsequences((String) donorGene.get(GENE_ID), donorGeneObservation);
      donorGeneObservation.setConsequence(consequences);

      unsetGeneId(donorGeneObservation);

      array.add(donorGeneObservation);
    }

    return gene;
  }

  private static void unsetGeneId(final org.icgc.dcc.release.job.document.model.Occurrence donorGeneObservation) {
    for (val consequence : donorGeneObservation.getConsequence()) {
      consequence.set_gene_id(null);
    }
  }

  @SuppressWarnings("unchecked")
  private static Collection<Occurrence> getTypeArray(Map<String, Object> gene, String observationType) {
    Collection<Occurrence> typeArray = (Collection<Occurrence>) gene.get(observationType);
    if (typeArray == null) {
      typeArray = Lists.newArrayList();
      gene.put(observationType, typeArray);
    }

    return typeArray;
  }

  /**
   * Returns consequences related to the target gene
   */
  private static Collection<Consequence> filterGeneObservationConsequences(String geneId, Occurrence geneObservation) {
    val consequences = geneObservation.getConsequence();
    if (consequences == null) {
      return Collections.emptyList();
    }

    val filteredConsequenes = Lists.<Consequence> newArrayList();
    for (val consequence : consequences) {
      val consequenceGeneId = firstNonNull(consequence.get_gene_id(), FAKE_GENE_ID);
      val related = geneId.equals(consequenceGeneId);

      if (related) {
        filteredConsequenes.add(consequence);
      }
    }

    return filteredConsequenes;
  }

  /**
   * Maps {@code _gene_id} to the donor's GeneSummary.
   */
  private static Map<String, Map<String, Object>> documentDonorGeneSummaries(Collection<Map<String, Object>> donorGenes) {
    val document = ImmutableMap.<String, Map<String, Object>> builder();

    boolean hasFake = false;
    for (val donorGene : donorGenes) {
      val donorGeneId = firstNonNull((String) donorGene.get(GENE_ID), FAKE_GENE_ID);
      if (isFakeGeneId(donorGeneId)) {
        hasFake = true;
      }

      document.put(donorGeneId, donorGene);
    }

    if (!hasFake) {
      // Special case since the summarize did not produce
      val fake = createFakeGene();
      donorGenes.add(fake);

      document.put(FAKE_GENE_ID, fake);
    }

    return document.build();
  }

  /**
   * Maps {@code _gene_id} to {@code occurrences}.
   */
  private static Multimap<String, Occurrence> documentDonorGenesObservations(Iterable<Occurrence> occurrences) {
    val document = ImmutableSetMultimap.<String, Occurrence> builder();

    for (val occurrence : occurrences) {
      val consequences = occurrence.getConsequence();

      if (consequences == null || consequences.isEmpty()) {
        document.put(FAKE_GENE_ID, occurrence);
      } else {
        for (val consequence : consequences) {
          document.put(resolveObservationConsequenceGeneId(consequence), occurrence);
        }
      }
    }

    return document.build();
  }

  private static String resolveObservationConsequenceGeneId(Consequence consequence) {
    val geneId = consequence.get_gene_id();

    return firstNonNull(geneId, FAKE_GENE_ID);
  }

  @SneakyThrows
  @SuppressWarnings("unchecked")
  private Map<String, Object> getGene(String donorGeneId) {
    if (FAKE_GENE_ID.equals(donorGeneId)) {
      return createFakeGene();
    }

    val gene = documentJobContext.getGenesBroadcast().value().get(donorGeneId);
    Map<String, Object> map = Maps.newHashMap();
    map = JacksonFactory.MAPPER.treeToValue(gene, map.getClass());

    return map;
  }

  private static Map<String, Object> createFakeGene() {
    val fakeGene = Maps.<String, Object> newHashMap();
    fakeGene.put(GENE_ID, FAKE_GENE_ID);
    fakeGene.put("fake", Boolean.TRUE);

    return fakeGene;
  }

  @SneakyThrows
  private static Donor convertDonor(ObjectNode row) {
    val donor = JacksonFactory.MAPPER.treeToValue(row, Donor.class);
    if (donor.getGene() == null) {
      donor.setGene(Lists.newLinkedList());
    }

    return donor;
  }

  private DocumentContext createContext(String donorId) {
    return new DonorCentricDocumentContext(donorId, documentJobContext, Optional.absent());
  }

  private void checkObjectClonnerInitialized() {
    if (objectClonner == null) {
      objectClonner = new Kryo();
    }
  }

}