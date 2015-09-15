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
package org.icgc.dcc.release.job.index.io;

import static com.fasterxml.jackson.core.JsonParser.Feature.AUTO_CLOSE_SOURCE;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.release.job.index.io.TarArchiveDocumentWriter.MAPPING_FILE_NAME;
import static org.icgc.dcc.release.job.index.io.TarArchiveDocumentWriter.SETTINGS_FILE_NAME;

import java.io.IOException;
import java.io.InputStream;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.icgc.dcc.release.job.index.core.Document;
import org.icgc.dcc.release.job.index.model.DocumentType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Reverse engineers the schema from Elasticsearch.
 */
@Slf4j
@RequiredArgsConstructor
public class TarArchiveDocumentReader {

  /**
   * Constants.
   */
  private static final ObjectMapper MAPPER = new ObjectMapper().configure(AUTO_CLOSE_SOURCE, false);

  /**
   * Configuration.
   */
  @NonNull
  private final InputStream inputStream;

  @SneakyThrows
  public void read(@NonNull DocumentType type, @NonNull TarArchiveEntryCallback callback) {
    val archiveStream = readArchiveStream(inputStream);

    int count = 0;
    TarArchiveEntry entry = null;
    while ((entry = archiveStream.getNextTarEntry()) != null) {
      if (isSettingsEntry(entry)) {

        val settings = readSource(archiveStream);

        callback.onSettings(settings);
      } else if (isMappingEntry(entry)) {
        val mapping = readSource(archiveStream);
        val mappingTypeName = getEntryNamePart(entry, 1);

        callback.onMapping(mappingTypeName, mapping);
      } else {
        val id = getEntryNamePart(entry, 2);
        val source = readSource(archiveStream);
        val document = new Document(type, id, source);

        // Dispatch
        callback.onDocument(document);

        if (++count % 1000 == 0) {
          log.info("Document count: {}", formatCount(count));
        }
      }
    }
  }

  private static String getEntryNamePart(TarArchiveEntry entry, int part) {
    return entry.getName().split("/")[part];
  }

  private static boolean isSettingsEntry(TarArchiveEntry entry) {
    return entry.getName().endsWith(SETTINGS_FILE_NAME);
  }

  private static boolean isMappingEntry(TarArchiveEntry entry) {
    return entry.getName().endsWith(MAPPING_FILE_NAME);
  }

  private static ObjectNode readSource(InputStream inputStream) throws IOException, JsonProcessingException {
    return (ObjectNode) MAPPER.readTree(inputStream);
  }

  private static TarArchiveInputStream readArchiveStream(InputStream inputStream) throws IOException {
    return new TarArchiveInputStream(inputStream);
  }

}
