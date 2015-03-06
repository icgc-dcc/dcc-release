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
package org.icgc.dcc.etl2.job.index.io;

import static com.google.common.base.Charsets.UTF_8;
import static org.icgc.dcc.common.core.util.FormatUtils.formatBytes;
import static org.icgc.dcc.etl2.job.index.factory.JacksonFactory.newDefaultMapper;

import java.io.IOException;
import java.io.OutputStream;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.icgc.dcc.etl2.job.index.core.Document;
import org.icgc.dcc.etl2.job.index.core.DocumentWriter;
import org.icgc.dcc.etl2.job.index.model.DocumentType;
import org.icgc.dcc.etl2.job.index.service.IndexService;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Knapsack compliant archive writer.
 * 
 * @see https://github.com/jprante/elasticsearch-knapsack
 */
@Slf4j
public class TarArchiveDocumentWriter implements DocumentWriter {

  /**
   * Constants.
   */
  private static final ObjectMapper MAPPER = newDefaultMapper();
  public static final String SETTINGS_FILE_NAME = "_settings";
  public static final String MAPPING_FILE_NAME = "_mapping";

  /**
   * Meta data.
   */
  private final String indexName;

  /**
   * State.
   */
  private final TarArchiveOutputStream archive;

  public TarArchiveDocumentWriter(@NonNull String indexName, @NonNull OutputStream outputStream) throws IOException {
    // Create state
    this.indexName = indexName;
    this.archive = createArchive(outputStream);

    addMetaEntries();
  }

  @Override
  public void write(Document document) throws IOException {
    val name = formatEntryName(document);
    val text = formatDocument(document);

    addEntry(name, text);
  }

  @Override
  public void close() throws IOException {
    log.info("Bytes written: {}", formatBytes(archive.getBytesWritten()));

    log.info("Closing archive...");
    archive.finish();
    archive.close();
    log.info("Finished!");
  }

  private void addMetaEntries() throws IOException {
    addEntry(SETTINGS_FILE_NAME, IndexService.getSettings());
    for (val type : DocumentType.values()) {
      val mappingEntryName = formatEntryName(type.getName(), MAPPING_FILE_NAME);

      addEntry(mappingEntryName, IndexService.getTypeMapping(type.getName()));
    }
  }

  private void addEntry(String name, String text) throws IOException {
    byte[] bytes = text.getBytes(UTF_8);

    // knapsack 2.x versions needs an extra directory so we embedded them under the index
    val entry = new TarArchiveEntry(formatEntryName(indexName, name));
    entry.setSize(bytes.length);

    archive.putArchiveEntry(entry);
    archive.write(bytes);
    archive.closeArchiveEntry();
  }

  private static String formatEntryName(Document document) {
    return formatEntryName(document.getType().getName(), document.getId());
  }

  private static String formatEntryName(String parent, String child) {
    return String.format("%s/%s", parent, child);
  }

  private static String formatDocument(Document document) throws JsonProcessingException {
    return MAPPER.writeValueAsString(document.getSource());
  }

  private static TarArchiveOutputStream createArchive(OutputStream outputStream) throws IOException {
    return new TarArchiveOutputStream(outputStream);
  }

}
