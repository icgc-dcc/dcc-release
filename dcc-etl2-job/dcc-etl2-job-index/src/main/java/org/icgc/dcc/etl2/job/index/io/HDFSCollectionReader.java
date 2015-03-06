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
package org.icgc.dcc.etl2.job.index.io;

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.etl2.core.util.MoreIterables.once;
import static org.icgc.dcc.etl2.core.util.ObjectNodes.MAPPER;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.core.model.ReleaseCollection;
import org.icgc.dcc.etl2.core.hadoop.FileGlobInputStream;
import org.icgc.dcc.etl2.core.job.FileType;
import org.icgc.dcc.etl2.core.util.ObjectNodeFilter;
import org.icgc.dcc.etl2.job.index.core.CollectionReader;
import org.icgc.dcc.etl2.job.index.model.CollectionFields;
import org.icgc.dcc.etl2.job.index.util.CollectionFieldsFilterAdapter;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ForwardingIterator;

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
    throw notReady();
  }

  @Override
  public Iterable<ObjectNode> readGeneSets(CollectionFields fields) {
    throw notReady();
  }

  @Override
  public Iterable<ObjectNode> readObservations(CollectionFields fields) {
    return read(FileType.OBSERVATION, fields);
  }

  @Override
  public Iterable<ObjectNode> readObservationsByDonorId(String donorId, CollectionFields fields) {
    throw notReady();
  }

  @Override
  public Iterable<ObjectNode> readObservationsByGeneId(String geneId, CollectionFields fields) {
    throw notReady();
  }

  @Override
  public Iterable<ObjectNode> readObservationsByMutationId(String mutationId, CollectionFields observationFields) {
    throw notReady();
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

  @SneakyThrows
  private Iterable<ObjectNode> read(FileType fileType, CollectionFields fields) {
    val fieldFilter = new CollectionFieldsFilterAdapter(fields);
    val filePath = getFilePath(fileType);
    val inputStream = new FileGlobInputStream(fileSystem, filePath);
    val iterator = lazyFilteredRead(inputStream, fieldFilter);

    return once(iterator);
  }

  private Path getFilePath(FileType fileType) throws IOException {
    val filePath = new Path(collectionDir, fileType.getDirName());
    checkState(fileSystem.exists(filePath), filePath);

    return filePath;
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

  private static Iterator<ObjectNode> lazyFilteredRead(final InputStream inputStream, final ObjectNodeFilter fieldFilter)
      throws IOException {
    val delegate = READER.<ObjectNode> readValues(inputStream);
    return new ForwardingIterator<ObjectNode>() {

      @Override
      @SneakyThrows
      protected Iterator<ObjectNode> delegate() {
        return delegate;
      }

      @Override
      public ObjectNode next() {
        val row = delegate().next();
        fieldFilter.filter(row);

        return row;
      }

    };
  }

  private static UnsupportedOperationException notReady() {
    throw new UnsupportedOperationException("HDFS collection suport not implemented!");
  }

}
