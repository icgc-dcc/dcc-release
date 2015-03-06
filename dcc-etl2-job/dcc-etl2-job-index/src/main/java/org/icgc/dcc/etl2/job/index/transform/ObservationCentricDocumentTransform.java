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
import static com.google.common.base.Strings.nullToEmpty;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_CANONICAL_TRANSCRIPT_ID;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES_CONSEQUENCE_CANONICAL;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES_GENE;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES_TRANSCRIPT_ID;
import static org.icgc.dcc.etl2.job.index.model.CollectionFieldAccessors.getDonorProjectId;
import static org.icgc.dcc.etl2.job.index.model.CollectionFieldAccessors.getObservationConsequenceGeneId;
import static org.icgc.dcc.etl2.job.index.model.CollectionFieldAccessors.getObservationConsequences;
import static org.icgc.dcc.etl2.job.index.model.CollectionFieldAccessors.getObservationDonorId;
import static org.icgc.dcc.etl2.job.index.model.CollectionFieldAccessors.getObservationType;
import static org.icgc.dcc.etl2.job.index.model.CollectionFieldAccessors.setObservationDonor;
import static org.icgc.dcc.etl2.job.index.model.CollectionFieldAccessors.setObservationProject;
import static org.icgc.dcc.etl2.job.index.util.Fakes.FAKE_GENE_ID;
import static org.icgc.dcc.etl2.job.index.util.Fakes.isFakeGeneId;

import java.util.TreeMap;
import java.util.UUID;

import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.etl2.job.index.core.Document;
import org.icgc.dcc.etl2.job.index.core.DocumentContext;
import org.icgc.dcc.etl2.job.index.core.DocumentTransform;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Maps;

/**
 * {@link DocumentTransform} implementation that creates a nested observation-centric document.
 */
public class ObservationCentricDocumentTransform implements DocumentTransform {

  @Override
  public Document transformDocument(@NonNull ObjectNode observation, @NonNull DocumentContext context) {
    // Indexes
    // - f(geneId) -> gene
    val observationGeneMap = newTreeMap();

    // Shorthands
    val observationDonorId = getObservationDonorId(observation);
    val observationType = getObservationType(observation);
    val observationConsequences = getObservationConsequences(observation);

    // Partition observations by type
    val observationPartition = observation.objectNode();
    observationPartition.with(observationType);
    observationPartition.put(observationType, observation);
    observation = observation.objectNode();
    observation.setAll(observationPartition);

    // Embed donor
    val observationDonor = context.getDonor(observationDonorId);
    setObservationDonor(observation, observationDonor);

    // Embed project
    val observationProjectId = getDonorProjectId(observationDonor);
    val observationProject = context.getProject(observationProjectId);
    setObservationProject(observation, observationProject);

    // Nest genes
    for (val value : observationConsequences) {
      val observationConsequence = (ObjectNode) value;
      val geneId = firstNonNull(getObservationConsequenceGeneId(observationConsequence), FAKE_GENE_ID);

      ObjectNode gene = observationGeneMap.get(geneId);
      if (gene == null) {
        // Book-keeping
        gene = isFakeGeneId(geneId) ? null : context.getGene(geneId).deepCopy();

        observationGeneMap.put(geneId, gene);
      }

      if (!isFakeGeneId(geneId)) {
        observationConsequence.set(OBSERVATION_CONSEQUENCES_GENE, gene);

        // Tag if it is canonical for efficient lookups downstream
        String canonicalTranscriptId = nullToEmpty(gene.path(GENE_CANONICAL_TRANSCRIPT_ID).textValue());
        String transcriptId = observationConsequence.path(OBSERVATION_CONSEQUENCES_TRANSCRIPT_ID).textValue();
        boolean canonical = transcriptId != null && transcriptId.equals(canonicalTranscriptId);
        observationConsequence.put(OBSERVATION_CONSEQUENCES_CONSEQUENCE_CANONICAL, canonical);
      }
    }

    return new Document(context.getType(), UUID.randomUUID().toString(), observation);
  }

  private static TreeMap<String, ObjectNode> newTreeMap() {
    return Maps.<String, ObjectNode> newTreeMap();
  }

}
