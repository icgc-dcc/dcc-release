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
package org.icgc.dcc.release.job.annotate.resolver;

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

import org.icgc.dcc.release.core.resolver.DirectoryResourceResolver;

@Slf4j
public class SnpEffJarResolver extends DirectoryResourceResolver<File> {

  /**
   * Configuration.
   */
  private final String version;

  public SnpEffJarResolver(@NonNull File resourceDir, @NonNull String version) {
    super(resourceDir);
    this.version = version;
  }

  @Override
  @SneakyThrows
  protected File get() {
    // TODO: Externalize
    val artifactId = "snpeff";
    val url = getUrl(artifactId, version);

    val dataDir = new File(getResourceDir(), "snpeff-jar");
    val versionDir = new File(dataDir, version);
    val jarFile = new File(versionDir, "snpeff.jar");
    if (jarFile.exists()) {
      return jarFile;
    }

    checkState(versionDir.mkdirs(), "Could not make data version directory '%s'", versionDir);

    log.info("Resolving SnpEff jar version '{}' from '{}' to '{}'...", new Object[] { version, url, jarFile });
    download(url, jarFile);
    log.info("Finished resolving SnfEff jar");

    return jarFile;
  }

  private void download(URL url, File jarFile) throws IOException, FileNotFoundException {
    @Cleanup
    val output = new FileOutputStream(jarFile);

    val channel = Channels.newChannel(url.openStream());
    output.getChannel().transferFrom(channel, 0, MAX_VALUE);
  }

  private URL getUrl(String artifactId, String version) throws MalformedURLException {
    val resourceUrl = "http://seqwaremaven.oicr.on.ca/artifactory/simple/dcc-dependencies";
    val groupPath = "ca/mcgill/mcb/pcingola/snpeff";
    val url = new URL(resourceUrl + "/" + groupPath + "/" + version + "/" + artifactId + "-" + version + ".jar");

    return url;
  }

}
