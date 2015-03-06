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
package org.icgc.dcc.etl2.job.annotate.resolver;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.Long.MAX_VALUE;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl2.core.resolver.DirectoryResourceResolver;

@Slf4j
public class SnpEffDatabaseResolver extends DirectoryResourceResolver<File> {

  /**
   * Configuration.
   */
  private final String resourceUrl;
  private final String version;

  public SnpEffDatabaseResolver(@NonNull File resourceDir, @NonNull String resourceUrl, @NonNull String version) {
    super(resourceDir);
    this.resourceUrl = resourceUrl;
    this.version = version;
  }

  @Override
  @SneakyThrows
  protected File get() {
    val artifactId = "dcc-snpeff";
    val url = getUrl(artifactId, version);

    val dataDir = new File(getResourceDir(), "snpeff");
    val versionDir = new File(dataDir, version);
    val databaseFile = new File(versionDir, "snpEffectPredictor.bin");
    if (databaseFile.exists()) {
      return dataDir;
    }

    checkState(versionDir.mkdirs(), "Could not make data version directory '%s'", versionDir);

    log.info("Resolving SnpEff database version '{}' from '{}' to '{}'...",
        new Object[] { version, url, databaseFile });
    download(url, databaseFile);
    log.info("Finished resolving SnfEff database");

    return dataDir;
  }

  private void download(URL url, File databaseFile) throws IOException, FileNotFoundException {
    @Cleanup
    val output = new FileOutputStream(databaseFile);

    val channel = Channels.newChannel(url.openStream());
    output.getChannel().transferFrom(channel, 0, MAX_VALUE);
  }

  private URL getUrl(String artifactId, String version) throws MalformedURLException {
    val url = new URL(resourceUrl + "/" + artifactId + "/" + version + "/" + artifactId + "-" + version + ".bin");

    return url;
  }

}
