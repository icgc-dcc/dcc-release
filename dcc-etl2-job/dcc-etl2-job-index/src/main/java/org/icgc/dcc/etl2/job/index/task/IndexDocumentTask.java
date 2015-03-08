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
package org.icgc.dcc.etl2.job.index.task;

import static com.google.common.io.Closeables.close;
import static org.icgc.dcc.etl2.job.index.factory.TransportClientFactory.newTransportClient;
import static org.icgc.dcc.etl2.job.index.model.DocumentType.MUTATION_CENTRIC_TYPE;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.etl2.core.task.RemoteActionTask;
import org.icgc.dcc.etl2.core.task.Task;
import org.icgc.dcc.etl2.core.task.TaskType;
import org.icgc.dcc.etl2.job.index.config.IndexProperties;
import org.icgc.dcc.etl2.job.index.core.CollectionReader;
import org.icgc.dcc.etl2.job.index.core.DocumentProcessor;
import org.icgc.dcc.etl2.job.index.core.DocumentWriter;
import org.icgc.dcc.etl2.job.index.io.ElasticSearchDocumentWriter;
import org.icgc.dcc.etl2.job.index.io.HDFSCollectionReader;
import org.icgc.dcc.etl2.job.index.io.MutationVCFDocumentWriter;
import org.icgc.dcc.etl2.job.index.io.TarArchiveDocumentWriter;
import org.icgc.dcc.etl2.job.index.model.DocumentType;
import org.icgc.dcc.etl2.job.index.util.DocumentWriterCallback;

import com.google.common.collect.ImmutableList;

@Slf4j
@RequiredArgsConstructor
public class IndexDocumentTask extends RemoteActionTask {

  /**
   * See
   * https://wiki.oicr.on.ca/display/DCCSOFT/Aggregated+Data+Download+Specification?focusedCommentId=57774680#comment
   * -57774680
   */
  private static final String VCF_FILE_NAME = "simple_somatic_mutation.aggregated.vcf.gz";

  @NonNull
  private final IndexProperties properties;
  @NonNull
  private final DocumentType type;

  @Override
  public String getName() {
    return Task.getName(this.getClass(), type.toString());
  }

  @Override
  public TaskType getType() {
    return TaskType.FILE_TYPE;
  }

  @Override
  @SneakyThrows
  protected void executeRemoteAction(FileSystem fileSystem, Path workingDir) {
    val writers = createWriters(type, fileSystem);
    val reader = createCollectionReader(fileSystem, workingDir);
    val processor = createProcessor(type, reader, writers);

    try {
      processor.process();
    } finally {
      // Cleanup
      val swallow = true;
      for (val writer : writers) {
        close(writer, swallow);
      }

      close(reader, swallow);
    }
  }

  protected Iterable<DocumentWriter> createWriters(DocumentType type, FileSystem fileSystem) throws IOException {
    val writers = ImmutableList.<DocumentWriter> builder();

    writers.add(createTarArchiveWriter(fileSystem));
    writers.add(createElasticSearchWriter(type));
    if (isVCFExportable(type)) {
      // Special case export
      writers.add(createVCFWriter(fileSystem));
    }

    return writers.build();
  }

  private HDFSCollectionReader createCollectionReader(FileSystem fileSystem, Path collectionDir) {
    return new HDFSCollectionReader(collectionDir, fileSystem);
  }

  protected DocumentWriter createTarArchiveWriter(FileSystem fileSystem) throws IOException {
    // Config
    val bufferSize = 8 * 1024;

    val archiveName = String.format("%s-%s.tar.gz", properties.getIndexName(), type.getName());
    val archivePath = new Path(properties.getOutputDir(), archiveName);
    if (fileSystem.exists(archivePath)) {
      log.info("Removing archive '{}'...", archivePath);
      fileSystem.delete(archivePath, false);
    }

    val archiveOutputStream =
        new GZIPOutputStream(new BufferedOutputStream(fileSystem.create(archivePath), bufferSize));

    log.info("Creating tar archive writer for archive file '{}'...", archivePath);
    return new TarArchiveDocumentWriter(properties.getIndexName(), archiveOutputStream);
  }

  protected DocumentWriter createVCFWriter(FileSystem fileSystem) throws IOException {
    // Config
    val bufferSize = 8 * 1024;

    int totalSsmTestedDonorCount = getTotalSsmTestedDonorCount();

    val vcfPath = new Path(properties.getOutputDir(), VCF_FILE_NAME);
    if (fileSystem.exists(vcfPath)) {
      log.info("Removing VCF file '{}'...", vcfPath);
      fileSystem.delete(vcfPath, false);
    }

    val vcfOutputStream =
        new GZIPOutputStream(new BufferedOutputStream(fileSystem.create(vcfPath), bufferSize));

    log.info("Creating VCF writer for path '{}'...", vcfPath);
    return new MutationVCFDocumentWriter(properties.getReleaseName(), properties.getIndexName(),
        properties.getFastaFile(),
        vcfOutputStream, totalSsmTestedDonorCount);
  }

  protected DocumentWriter createElasticSearchWriter(DocumentType type) {
    // Sets the number of concurrent requests allowed to be executed. A value of 0 means that only a single request will
    // be allowed to be executed. A value of 1 means 1 concurrent request is allowed to be executed while accumulating
    // new bulk requests.
    val concurrentRequests = 1; // 2014-08-26: Changed from 10 to 1 to reduce chance for OOM
    return new ElasticSearchDocumentWriter(newTransportClient(properties.getEsUri()), properties.getIndexName(), type,
        concurrentRequests);
  }

  protected DocumentProcessor createProcessor(DocumentType type, CollectionReader reader,
      Iterable<DocumentWriter> writers) {
    val processor = new DocumentProcessor(properties.getIndexName(), type, reader);

    for (val writer : writers) {
      processor.addCallback(new DocumentWriterCallback(writer));
    }

    return processor;
  }

  private boolean isVCFExportable(DocumentType type) {
    return properties.isExportVCF() && type == MUTATION_CENTRIC_TYPE;
  }

  private int getTotalSsmTestedDonorCount() throws IOException {
    throw new UnsupportedOperationException(
        "Need to use collection reader to stream through aggregated projects. See dcc-etl-indexer.ProjectRepository");
  }

}
