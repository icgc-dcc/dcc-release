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
package org.icgc.dcc.release.core.resolver;

import static com.google.common.base.Preconditions.checkState;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.zip.GZIPInputStream;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.tools.tar.TarEntry;
import org.apache.tools.tar.TarInputStream;

@Slf4j
public class ReferenceGenomeResolver extends DirectoryResourceResolver<File> {

  /**
   * Configuration.
   */
  private final String resourceUrl;
  private final String version;

  public ReferenceGenomeResolver(@NonNull File resourceDir, @NonNull String resourceUrl, @NonNull String version) {
    super(resourceDir);
    this.resourceUrl = resourceUrl;
    this.version = version;
  }

  @Override
  @SneakyThrows
  protected File get() {
    val artifactId = "dcc-reference-genome";
    val url = getUrl(artifactId, version);

    val dataDir = new File(getResourceDir(), "genome");
    val versionDir = new File(dataDir, version);
    val fastaFile = new File(versionDir, version + ".fasta");

    synchronized (this.getClass()){
      if (fastaFile.exists()) {
        log.info("Reference genome '{}' exists. Returning", fastaFile.getAbsolutePath());
        return fastaFile;
      }

      if(!versionDir.exists()) {
        log.info("Reference genome '{}' does not exist. Creating...", versionDir.getAbsolutePath());
        checkState(versionDir.mkdirs(), "Could not make data version directory '%s'", versionDir);
      }

      log.info("Downloading reference genome version '{}'...", version);
      download(url, versionDir);
      log.info("Finished downloading reference genome");

      return fastaFile;

    }
  }

  private void download(URL url, File dir) throws IOException, FileNotFoundException {
    @Cleanup
    val tar = new TarInputStream(new GZIPInputStream(url.openStream()));

    TarEntry entry;

    while ((entry = tar.getNextEntry()) != null) {
      val name = entry.getName();
      val file = new File(dir, entry.getName());

      @Cleanup
      val output = new FileOutputStream(file);

      log.info("Extracting '{}' to '{}'...", name, file);
      tar.copyEntryContents(output);
      log.info("Finished extracting '{}' to '{}'...", name, file);
    }
  }

  private URL getUrl(String artifactId, String version) throws MalformedURLException {
    val url = new URL(resourceUrl + "/" + artifactId + "/" + version + "/" + artifactId + "-" + version + ".tar.gz");

    return url;
  }

}
