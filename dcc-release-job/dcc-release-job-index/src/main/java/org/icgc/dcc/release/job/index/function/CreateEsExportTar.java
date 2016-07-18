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
package org.icgc.dcc.release.job.index.function;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static org.icgc.dcc.common.core.util.Joiners.PATH;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.icgc.dcc.common.core.util.Separators;
import org.icgc.dcc.common.hadoop.fs.Configurations;
import org.icgc.dcc.common.hadoop.fs.FileSystems;
import org.icgc.dcc.release.core.document.Document;
import org.icgc.dcc.release.core.util.JacksonFactory;
import org.icgc.dcc.release.job.index.service.IndexService;
import org.icgc.dcc.release.job.index.task.EsExportTask;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Slf4j
@RequiredArgsConstructor
public final class CreateEsExportTar implements FlatMapFunction<Iterator<Document>, Void> {

  private static final ObjectMapper MAPPER = JacksonFactory.MAPPER;

  @NonNull
  private final String indexName;
  @NonNull
  private final String workingDir;
  @NonNull
  private final String documentType;
  @NonNull
  private final Map<String, String> fileSystemSettings;

  @Override
  public Iterable<Void> call(Iterator<Document> documents) throws Exception {
    if (!documents.hasNext()) {
      log.warn("Empty partition.");

      return Collections.emptyList();
    }

    val archivePath = getOutputPath(getArchiveName());
    @Cleanup
    val tarOutputStream = getOutputStream(archivePath);
    addMeta(tarOutputStream);

    while (documents.hasNext()) {
      val document = documents.next();
      writeDocument(tarOutputStream, document);
    }
    tarOutputStream.finish();

    return Collections.emptyList();
  }

  private void writeDocument(TarArchiveOutputStream tarOutputStream, Document document) throws Exception {
    val documentId = document.getId();
    checkState(document.getType().getName().equals(documentType),
        "Document '%s' doesn't belong to archive with document type '%s'", documentId, documentType);
    val source = document.getSource();
    val fileName = getDocumentFileName(documentId);
    writeEntry(tarOutputStream, source, fileName);
  }

  private void addMeta(TarArchiveOutputStream tarOutputStream) throws Exception {
    addSettings(tarOutputStream);
    addMappings(tarOutputStream);
  }

  private void addMappings(TarArchiveOutputStream tarOutputStream) throws Exception {
    val mapping = IndexService.getTypeMapping(documentType);
    val fileName = getDocumentFileName("_mapping");
    writeEntry(tarOutputStream, mapping, fileName);
  }

  private void addSettings(TarArchiveOutputStream tarOutputStream) throws Exception {
    val settings = IndexService.getSettings();
    val fileName = PATH.join(indexName, "_settings");
    writeEntry(tarOutputStream, settings, fileName);
  }

  private String getDocumentFileName(String documentId) {
    return PATH.join(indexName, documentType, documentId);
  }

  private String getArchiveName() {
    return format("%s_%s.tar.gz", indexName.toLowerCase(), documentType);
  }

  private Path getOutputPath(String fileName) {
    return new Path(workingDir + Separators.PATH + EsExportTask.ES_EXPORT_DIR + Separators.PATH + fileName);
  }

  private TarArchiveOutputStream getOutputStream(Path archivePath) throws IOException {
    val configuration = Configurations.fromMap(fileSystemSettings);
    val fileSystem = FileSystems.getFileSystem(configuration);
    val archiveOutputStream = new GZIPOutputStream(new BufferedOutputStream(fileSystem.create(archivePath)));
    log.info("Creating tar archive writer for archive file '{}'...", archivePath);

    return createTarOutputStream(archiveOutputStream);
  }

  private static void writeEntry(TarArchiveOutputStream tarOutputStream, ObjectNode source, String fileName)
      throws Exception {
    byte[] sourceBytes = MAPPER.writeValueAsBytes(source);
    val entry = createTarEntry(fileName, sourceBytes.length);
    tarOutputStream.putArchiveEntry(entry);
    tarOutputStream.write(sourceBytes);
    tarOutputStream.closeArchiveEntry();
  }

  private static TarArchiveEntry createTarEntry(String file, long size) {
    val entry = new TarArchiveEntry(file);
    entry.setSize(size);

    return entry;
  }

  private static TarArchiveOutputStream createTarOutputStream(@NonNull OutputStream outputStream) {
    log.debug("Creating tar output stream...");
    val tarOutputStream = new TarArchiveOutputStream(outputStream);
    tarOutputStream.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
    tarOutputStream.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_POSIX);

    return tarOutputStream;
  }

}