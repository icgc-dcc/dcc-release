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
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_PROJECT_ID;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES_GENE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_DONOR_ID;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_GENE;
import static org.icgc.dcc.common.json.Jackson.asObjectNode;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getDonorProjectId;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getObservationConsequenceGeneId;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getObservationConsequences;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getObservationDonorId;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getObservationType;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.setObservationDonor;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.setObservationProject;
import static org.icgc.dcc.release.job.document.util.Fakes.FAKE_GENE_ID;
import static org.icgc.dcc.release.job.document.util.Fakes.createFakeGene;
import static org.icgc.dcc.release.job.document.util.Fakes.isFakeGeneId;

import java.util.TreeMap;
import java.util.UUID;

import lombok.NonNull;
import lombok.val;

import org.apache.spark.api.java.function.Function;
import org.icgc.dcc.release.core.document.Document;
import org.icgc.dcc.release.core.document.DocumentType;
import org.icgc.dcc.release.job.document.context.DefaultDocumentContext;
import org.icgc.dcc.release.job.document.core.DocumentContext;
import org.icgc.dcc.release.job.document.core.DocumentJobContext;
import org.icgc.dcc.release.job.document.core.DocumentTransform;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Maps;

/**
 * {@link DocumentTransform} implementation that creates a nested observation-centric document.
 */
public class ObservationCentricDocumentTransform implements DocumentTransform, Function<ObjectNode, Document> {

  private final DocumentContext documentContext;

  public ObservationCentricDocumentTransform(@NonNull DocumentJobContext documentJobContext) {
    this.documentContext = new DefaultDocumentContext(DocumentType.OBSERVATION_CENTRIC_TYPE, documentJobContext);
  }

  @Override
  public Document call(ObjectNode observation) throws Exception {
    return transformDocument(observation, documentContext);
  }

  @Override
  public Document transformDocument(@NonNull ObjectNode observation, @NonNull DocumentContext context) { // NOPMD
    // Indexes
    // - f(geneId) -> gene
    val observationGeneMap = newTreeMap();

    // Shorthands
    val observationDonorId = getObservationDonorId(observation);
    val observationType = getObservationType(observation);
    val observationConsequences = getObservationConsequences(observation);

    // Remove foreign keys
    trimObservation(observation);
    observation.remove(OBSERVATION_CONSEQUENCES);

    // Partition observations by type
    val observationPartition = observation.objectNode();
    observationPartition.with(observationType);
    observationPartition.set(observationType, observation);
    observation = observation.objectNode();
    observation.setAll(observationPartition);

    // Embed donor
    val observationDonor = context.getDonor(observationDonorId).deepCopy();
    setObservationDonor(observation, observationDonor);

    // Embed project
    val observationProjectId = getDonorProjectId(observationDonor);
    trimObservationDonor(observationDonor);
    val observationProject = context.getProject(observationProjectId);
    setObservationProject(observation, observationProject);

    // Nest genes
    for (val value : observationConsequences) {
      val observationConsequence = (ObjectNode) value;
      val geneId = firstNonNull(getObservationConsequenceGeneId(observationConsequence), FAKE_GENE_ID);

      ObjectNode gene = observationGeneMap.get(geneId);
      if (gene == null) {
        // Book-keeping
        gene = isFakeGeneId(geneId) ? createFakeGene() : context.getGene(geneId).deepCopy();

        observationGeneMap.put(geneId, gene);
      }

      trimObservationConsequence(observationConsequence);
      val consequences = gene.withArray(OBSERVATION_CONSEQUENCES);
      consequences.add(observationConsequence);
    }

    val observationGenes = createGenesArray(observation, observationType);
    for (val gene : observationGeneMap.values()) {
      observationGenes.add(gene);
    }

    return new Document(context.getType(), UUID.randomUUID().toString(), observation);
  }

  private static ArrayNode createGenesArray(ObjectNode observation, String observationType) {
    return asObjectNode(observation.get(observationType)).withArray(OBSERVATION_GENE);
  }

  private static void trimObservation(ObjectNode observation) {
    observation.remove(OBSERVATION_DONOR_ID);
  }

  private static void trimObservationDonor(ObjectNode observationDonor) {
    observationDonor.remove(DONOR_PROJECT_ID);
  }

  private static void trimObservationConsequence(ObjectNode observationConsequence) {
    observationConsequence.remove(OBSERVATION_CONSEQUENCES_GENE_ID);
  }

  private static TreeMap<String, ObjectNode> newTreeMap() {
    return Maps.<String, ObjectNode> newTreeMap();
  }

}
