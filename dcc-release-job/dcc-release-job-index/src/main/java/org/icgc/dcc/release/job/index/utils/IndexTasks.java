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
package org.icgc.dcc.release.job.index.utils;

import static com.google.common.base.Preconditions.checkState;
import static lombok.AccessLevel.PRIVATE;
import static org.apache.hadoop.fs.Path.SEPARATOR;
import static org.icgc.dcc.common.core.util.Separators.UNDERSCORE;

import java.util.List;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.val;

import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.core.util.Separators;
import org.icgc.dcc.common.core.util.Splitters;
import org.icgc.dcc.release.core.document.DocumentType;

@NoArgsConstructor(access = PRIVATE)
public final class IndexTasks {

  public static final String GZIP_EXTENSION = ".gz";
  private static final String BIG_FILES_DIR = "big_files";

  public static String getBigFilesDir(@NonNull String workingDir) {
    return workingDir + SEPARATOR + BIG_FILES_DIR;
  }

  public static Path getBigFilesPath(@NonNull String workingDir) {
    return new Path(getBigFilesDir(workingDir));
  }

  public static String getBigFileName(@NonNull DocumentType documentType, @NonNull String id) {
    return documentType.getName() + UNDERSCORE + id + GZIP_EXTENSION;
  }

  public static String getIndexName(@NonNull String releaseName) {
    return releaseName.toLowerCase();
  }

  public static DocumentType getDocumentTypeFromFileName(@NonNull String fileName) {
    val parts = getParts(fileName);

    return DocumentType.byName(parts.get(0));
  }

  public static String getIdFromFileName(@NonNull String fileName) {
    val parts = getParts(fileName);

    return parts.get(1).replace(GZIP_EXTENSION, Separators.EMPTY_STRING);
  }

  private static List<String> getParts(String fileName) {
    val parts = Splitters.UNDERSCORE.splitToList(fileName);
    checkState(parts.size() == 2, "Failed to resolve DocumentType from file name %s", fileName);

    return parts;
  }

}
