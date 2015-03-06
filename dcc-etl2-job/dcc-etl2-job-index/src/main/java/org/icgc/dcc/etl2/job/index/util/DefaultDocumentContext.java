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
package org.icgc.dcc.etl2.job.index.util;

import static org.icgc.dcc.etl2.core.util.Stopwatches.createStarted;
import static org.icgc.dcc.etl2.job.index.model.CollectionFieldAccessors.getDonorId;
import static org.icgc.dcc.etl2.job.index.model.CollectionFieldAccessors.getGeneGeneSetId;
import static org.icgc.dcc.etl2.job.index.model.CollectionFieldAccessors.getGeneGeneSets;
import static org.icgc.dcc.etl2.job.index.model.CollectionFieldAccessors.getGeneId;
import static org.icgc.dcc.etl2.job.index.model.CollectionFieldAccessors.getGeneSetId;
import static org.icgc.dcc.etl2.job.index.model.CollectionFieldAccessors.getProjectId;
import static org.icgc.dcc.etl2.job.index.util.Fakes.FAKE_GENE_ID;
import static org.icgc.dcc.etl2.job.index.util.Fakes.createFakeGene;

import java.util.Map;

import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl2.job.index.core.CollectionReader;
import org.icgc.dcc.etl2.job.index.core.DocumentContext;
import org.icgc.dcc.etl2.job.index.model.DocumentType;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Multimap;

/**
 * Default implementation of the {@link DocumentContext} abstraction.
 */
@Value
@Slf4j
public class DefaultDocumentContext implements DocumentContext {

  /**
   * Index name under construction.
   */
  @NonNull
  private final String indexName;

  /**
   * Document type under construction.
   */
  @NonNull
  private final DocumentType type;

  /**
   * Data access for document inputs.
   */
  @NonNull
  private final CollectionReader reader;

  /**
   * Lazy-loaded map of projects indexed by project id.
   */
  @Getter(lazy = true)
  private final Map<String, ObjectNode> projects = resolveProjects();

  /**
   * Lazy-loaded map of donors indexed by donor id.
   */
  @Getter(lazy = true)
  private final Map<String, ObjectNode> donors = resolveDonors();

  /**
   * Lazy-loaded map of genes indexed by gene id.
   */
  @Getter(lazy = true)
  private final Map<String, ObjectNode> genes = resolveGenes();

  /**
   * Lazy-loaded map of gene sets indexed by gene set id.
   */
  @Getter(lazy = true)
  private final Map<String, ObjectNode> geneSets = resolveGeneSets();

  /**
   * Lazy-loaded map of genes indexed by gene id.
   * 
   */
  @Getter(lazy = true)
  private final Multimap<String, ObjectNode> geneSetGeneIdIndex = resolveGeneSetGeneIdIndex();

  @Override
  public ObjectNode getProject(@NonNull String projectId) {
    return getProjects().get(projectId);
  }

  @Override
  public ObjectNode getDonor(@NonNull String donorId) {
    return getDonors().get(donorId);
  }

  @Override
  public ObjectNode getGene(@NonNull String geneId) {
    return getGenes().get(geneId);
  }

  @Override
  public Iterable<ObjectNode> getGenesByGeneSetId(String geneSetId) {
    return getGeneSetGeneIdIndex().get(geneSetId);
  }

  @Override
  public Iterable<ObjectNode> getObservationsByDonorId(@NonNull String donorId) {
    return readObservationsByDonorId(donorId);
  }

  @Override
  public Iterable<ObjectNode> getObservationsByGeneId(@NonNull String geneId) {
    return readObservationsByGeneId(geneId);
  }

  @Override
  public Iterable<ObjectNode> getObservationsByMutationId(@NonNull String mutationId) {
    return readObservationsByMutationId(mutationId);
  }

  private Map<String, ObjectNode> resolveProjects() {
    val watch = createStarted();
    log.info("Resolving projects...");
    val projects = createMap();
    for (val project : readProjects()) {
      projects.put(getProjectId(project), project);
    }

    log.info("Resolved projects in {}", watch);
    return projects.build();
  }

  private Map<String, ObjectNode> resolveDonors() {
    val watch = createStarted();
    log.info("Resolving donors...");
    val donors = createMap();
    for (val donor : readDonors()) {
      donors.put(getDonorId(donor), donor);
    }

    log.info("Resolved donors in {}", watch);
    return donors.build();
  }

  private Map<String, ObjectNode> resolveGenes() {
    val watch = createStarted();
    log.info("Resolving genes...");
    val genes = createMap();
    for (val gene : readGenes()) {
      // Add "real" gene
      genes.put(getGeneId(gene), gene);
    }

    // Add "fake" gene into the "real" gene pool
    genes.put(FAKE_GENE_ID, createFakeGene());

    log.info("Resolved genes in {}", watch);
    return genes.build();
  }

  private Map<String, ObjectNode> resolveGeneSets() {
    val watch = createStarted();
    log.info("Resolving gene sets...");
    val geneSets = createMap();
    for (val geneSet : readGeneSets()) {
      geneSets.put(getGeneSetId(geneSet), geneSet);
    }

    log.info("Resolved gene sets in {}", watch);
    return geneSets.build();
  }

  private Multimap<String, ObjectNode> resolveGeneSetGeneIdIndex() {
    val index = HashMultimap.<String, ObjectNode> create();

    for (val gene : getGenes().values()) {
      for (val geneGeneSet : getGeneGeneSets(gene)) {
        val geneGeneSetId = getGeneGeneSetId(geneGeneSet);

        // Reverse lookup by gene set id
        index.put(geneGeneSetId, gene);
      }
    }

    return index;
  }

  private Iterable<ObjectNode> readProjects() {
    return reader.readProjects(type.getFields().getProjectFields());
  }

  private Iterable<ObjectNode> readDonors() {
    return reader.readDonors(type.getFields().getDonorFields());
  }

  private Iterable<ObjectNode> readGenes() {
    // NOTE: Special case
    return reader.readGenesPivoted(type.getFields().getGeneFields());
  }

  private Iterable<ObjectNode> readGeneSets() {
    return reader.readGeneSets(type.getFields().getGeneSetFields());
  }

  private Iterable<ObjectNode> readObservationsByDonorId(String donorId) {
    return reader.readObservationsByDonorId(donorId, type.getFields().getObservationFields());
  }

  private Iterable<ObjectNode> readObservationsByGeneId(String donorId) {
    return reader.readObservationsByGeneId(donorId, type.getFields().getObservationFields());
  }

  private Iterable<ObjectNode> readObservationsByMutationId(String mutationId) {
    return reader.readObservationsByMutationId(mutationId, type.getFields().getObservationFields());
  }

  /**
   * Helper to reduce repeated type noise.
   */
  private static Builder<String, ObjectNode> createMap() {
    return ImmutableMap.<String, ObjectNode> builder();
  }

}