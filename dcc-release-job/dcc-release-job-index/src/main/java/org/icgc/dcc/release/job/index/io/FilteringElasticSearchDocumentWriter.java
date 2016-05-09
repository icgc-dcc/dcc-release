/*
 * Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.                             
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

import static org.icgc.dcc.release.job.index.utils.IndexTasks.getBigFileName;
import static org.icgc.dcc.release.job.index.utils.IndexTasks.getBigFilesDir;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.Client;
import org.icgc.dcc.common.hadoop.fs.FileSystems;
import org.icgc.dcc.common.hadoop.fs.HadoopUtils;
import org.icgc.dcc.release.core.document.Document;
import org.icgc.dcc.release.core.util.JacksonFactory;

/**
 * Filters out big documents and writes them to file system.
 */
@Slf4j
public class FilteringElasticSearchDocumentWriter extends ElasticSearchDocumentWriter {

  /**
   * Configuration.
   */
  private final int threshold;
  private final String workingDir;
  private final Map<String, String> fsSettings;

  /**
   * Dependencies.
   */
  private FileSystem fileSystem;

  public FilteringElasticSearchDocumentWriter(
      Client client,
      String indexName,
      IndexingState indexingState,
      ClusterStateVerifier clusterStateVerifier,
      BulkProcessor bulkProcessor,
      String writerId,
      int threshold,
      String workingDir,
      Map<String, String> fsSettings) {
    super(client, indexName, indexingState, bulkProcessor, writerId);
    this.threshold = threshold * 1024 * 1024;
    this.workingDir = getBigFilesDir(workingDir);
    this.fsSettings = fsSettings;
  }

  @Override
  public void write(Document document) throws IOException {
    byte[] source = createSource(document.getSource());
    if (isBigDocument(source.length)) {
      writeToFileSystem(document);
    } else {
      write(document.getId(), document.getType(), source);
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
    if (fileSystem != null) {
      log.info("Closing a FileSystem...");
      fileSystem.close();
    }
  }

  private boolean isBigDocument(int length) {
    return length > threshold;
  }

  @SneakyThrows
  private void writeToFileSystem(Document document) {
    val path = getPath(document);
    log.info("Saving big file to {}", path);
    @Cleanup
    val out = getOutputStream(path);
    JacksonFactory.SMILE_WRITER.writeValue(out, document.getSource());
  }

  private OutputStream getOutputStream(Path path) throws IOException {
    return new GZIPOutputStream(getFileSystem().create(path));
  }

  private Path getPath(Document document) {
    ensurePath(workingDir);

    return new Path(workingDir, getName(document));
  }

  private void ensurePath(String workingDir) {
    if (!HadoopUtils.checkExistence(getFileSystem(), workingDir)) {
      HadoopUtils.mkdirs(getFileSystem(), workingDir);
    }
  }

  private static String getName(Document document) {
    val id = document.getId();
    val type = document.getType();

    return getBigFileName(type, id);
  }

  private FileSystem getFileSystem() {
    if (fileSystem == null) {
      fileSystem = FileSystems.getFileSystem(fsSettings);
    }

    return fileSystem;
  }

}
