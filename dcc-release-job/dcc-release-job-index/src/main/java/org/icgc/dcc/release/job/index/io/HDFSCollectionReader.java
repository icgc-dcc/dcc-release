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
package org.icgc.dcc.release.job.index.io;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterators.filter;
import static com.google.common.collect.Iterators.transform;
import static org.icgc.dcc.release.core.util.MoreIterables.once;
import static org.icgc.dcc.release.core.util.ObjectNodes.MAPPER;
import static org.icgc.dcc.release.job.index.model.CollectionFieldAccessors.getDonorId;
import static org.icgc.dcc.release.job.index.model.CollectionFieldAccessors.getObservationConsequenceGeneIds;
import static org.icgc.dcc.release.job.index.model.CollectionFieldAccessors.getObservationMutationId;
import static org.icgc.dcc.release.job.index.model.CollectionFields.collectionFields;
import static org.icgc.dcc.release.job.index.transform.GeneGeneSetPivoter.pivotGenesGeneSets;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.core.model.ReleaseCollection;
import org.icgc.dcc.release.core.hadoop.FileGlobInputStream;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.util.ObjectNodeFilter;
import org.icgc.dcc.release.job.index.core.CollectionReader;
import org.icgc.dcc.release.job.index.model.CollectionFields;
import org.icgc.dcc.release.job.index.util.CollectionFieldsFilterAdapter;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;

@RequiredArgsConstructor
public class HDFSCollectionReader implements CollectionReader {

  /**
   * Constants.
   */
  public static final ObjectReader READER = MAPPER.reader(ObjectNode.class);

  /**
   * Configuration.
   */
  private final Path collectionDir;

  /**
   * Dependencies.
   */
  private final FileSystem fileSystem;

  @Override
  public Iterable<ObjectNode> readReleases(@NonNull CollectionFields fields) {
    return read(FileType.RELEASE, fields);
  }

  @Override
  public Iterable<ObjectNode> readProjects(CollectionFields fields) {
    return read(FileType.PROJECT, fields);
  }

  @Override
  public Iterable<ObjectNode> readDonors(CollectionFields fields) {
    return read(FileType.DONOR, fields);
  }

  @Override
  public Iterable<ObjectNode> readGenes(CollectionFields fields) {
    return read(FileType.GENE, fields);
  }

  @Override
  public Iterable<ObjectNode> readGenesPivoted(CollectionFields fields) {
    val genes = readGenes(fields);

    // TODO: This is somewhat of hack, but it was the most expeditious way to get the desired result
    return pivotGenesGeneSets(genes, readGeneSetOntologies());
  }

  @Override
  public Iterable<ObjectNode> readGeneSets(CollectionFields fields) {
    return read(FileType.GENE_SET, fields);
  }

  @Override
  public Iterable<ObjectNode> readObservations(CollectionFields fields) {
    return read(FileType.OBSERVATION, fields);
  }

  @Override
  public Iterable<ObjectNode> readObservationsByDonorId(String donorId, CollectionFields fields) {
    return read(FileType.OBSERVATION, fields, row -> getDonorId(row).equals(donorId));
  }

  @Override
  public Iterable<ObjectNode> readObservationsByGeneId(String geneId, CollectionFields fields) {
    return read(FileType.OBSERVATION, fields, row -> {
      for (String observationGeneId : getObservationConsequenceGeneIds(row)) {
        if (observationGeneId != null && observationGeneId.equals(geneId)) {
          return true;
        }
      }

      return false;
    });
  }

  @Override
  public Iterable<ObjectNode> readObservationsByMutationId(String mutationId, CollectionFields fields) {
    return read(FileType.OBSERVATION, fields, row -> getObservationMutationId(row).equals(mutationId));
  }

  @Override
  public Iterable<ObjectNode> readMutations(CollectionFields fields) {
    return read(FileType.MUTATION, fields);
  }

  @Override
  public Iterable<ObjectNode> read(ReleaseCollection collection, CollectionFields fields) {
    return read(resolveFileType(collection), fields);
  }

  @Override
  public void close() throws IOException {
    // No-op
  }

  protected Map<String, String> readGeneSetOntologies() {
    val geneSets = readGeneSets(collectionFields().includedFields("id", "go_term.ontology").build());

    val geneSetOntologies = ImmutableMap.<String, String> builder();
    for (val geneSet : geneSets) {
      val goTerm = geneSet.path("go_term");

      if (!goTerm.isMissingNode()) {
        val id = geneSet.get("id").textValue();
        val ontology = goTerm.get("ontology").textValue();

        geneSetOntologies.put(id, ontology);
      }
    }

    return geneSetOntologies.build();
  }

  private Iterable<ObjectNode> read(FileType fileType, CollectionFields fields) {
    return read(fileType, fields, row -> true);
  }

  private Iterable<ObjectNode> read(FileType fileType, CollectionFields fields, Predicate<ObjectNode> rowFilter) {
    val fieldFilter = new CollectionFieldsFilterAdapter(fields);
    val filePath = getFilePath(fileType);
    val inputStream = new FileGlobInputStream(fileSystem, filePath);
    val iterator = lazyFilteredRead(inputStream, fieldFilter, rowFilter);

    return once(iterator);
  }

  @SneakyThrows
  private Path getFilePath(FileType fileType) {
    val basePath = new Path(collectionDir, fileType.getDirName());
    checkState(fileSystem.exists(basePath), basePath);

    return fileType.isPartitioned() ? new Path(basePath, "*") : basePath;
  }

  private static FileType resolveFileType(ReleaseCollection collection) {
    if (collection == ReleaseCollection.DONOR_COLLECTION) {
      return FileType.DONOR;
    } else if (collection == ReleaseCollection.GENE_COLLECTION) {
      return FileType.GENE;
    } else if (collection == ReleaseCollection.GENE_SET_COLLECTION) {
      return FileType.GENE_SET;
    } else if (collection == ReleaseCollection.MUTATION_COLLECTION) {
      return FileType.MUTATION;
    } else if (collection == ReleaseCollection.OBSERVATION_COLLECTION) {
      return FileType.OBSERVATION;
    } else if (collection == ReleaseCollection.PROJECT_COLLECTION) {
      return FileType.PROJECT;
    } else if (collection == ReleaseCollection.RELEASE_COLLECTION) {
      return FileType.RELEASE;
    }

    throw new IllegalArgumentException(collection + " not expected");
  }

  @SneakyThrows
  private static Iterator<ObjectNode> lazyFilteredRead(InputStream inputStream, ObjectNodeFilter fieldFilter,
      Predicate<ObjectNode> rowFilter) {
    val delegate = READER.<ObjectNode> readValues(inputStream);

    // Filter rows
    val filtered = filter(delegate, rowFilter);

    // Filter fields
    return transform(filtered, row -> fieldFilter.filter(row));
  }

}
