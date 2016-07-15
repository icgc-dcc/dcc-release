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

import static java.lang.String.format;

import java.io.OutputStream;
import java.util.Collections;
import java.util.Iterator;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.icgc.dcc.common.core.util.Separators;
import org.icgc.dcc.release.core.document.Document;

@Slf4j
@RequiredArgsConstructor
public final class CreateEsExportTar implements FlatMapFunction<Iterator<Document>, Void> {

  private static final String ES_EXPORT_DIR = "es_export";

  @NonNull
  private final String indexName;
  @NonNull
  private final String workingDir;

  @Override
  public Iterable<Void> call(Iterator<Document> documents) throws Exception {
    if (!documents.hasNext()) {
      log.warn("Empty partition.");

      return Collections.emptyList();
    }

    val firstDoc = documents.next();
    val archivePath = getOutputPath(getArchiveName(firstDoc));
    // FIXME: finish

    // Insert settings
    // Insert mappings

    return Collections.emptyList();
  }

  private String getArchiveName(Document doc) {
    return format("%s_%s.tar.gz", indexName.toLowerCase(), doc.getType().getName());
  }

  private Path getOutputPath(String fileName) {
    return new Path(workingDir + Separators.PATH + ES_EXPORT_DIR + Separators.PATH + fileName);
  }

  private static TarArchiveOutputStream createTarOutputStream(@NonNull OutputStream outputStream) {
    log.debug("Creating tar output stream...");
    val tarOutputStream = new TarArchiveOutputStream(outputStream);
    tarOutputStream.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
    tarOutputStream.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_POSIX);

    return tarOutputStream;
  }

}