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
package org.icgc.dcc.release.job.annotate.resolver;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.io.Files;

@Slf4j
public class SnpEffDatabaseResolverTest {

  private static final String VERSION = "4.2-GRCh37.85";
  private static final String RESOURCE_URL = "src/test/resources/fixtures";

  File resourceDir;

  SnpEffDatabaseResolver resolver;

  @Before
  public void setUp() throws Exception {
    resourceDir = Files.createTempDir();
    val resource = new File(new File("."), RESOURCE_URL);

    val url = "file://" + resource.getAbsolutePath();
    log.info("Resource URL {}", url);

    resolver = new SnpEffDatabaseResolver(resourceDir, url, VERSION);
  }

  @Test
  @Ignore("For testing download from the Artifactory")
  public void integrationTestGet() {
    val artifactoryUrl = "https://artifacts.oicr.on.ca/artifactory/simple/dcc-dependencies/org/icgc/dcc";
    resolver = new SnpEffDatabaseResolver(resourceDir, artifactoryUrl, VERSION);

    val dir = resolver.resolve();
    verifySnpEffDirectoryStructure(dir);
  }

  @Test
  public void testGet() throws Exception {
    val dir = resolver.resolve();
    verifySnpEffDirectoryStructure(dir);
  }

  private void verifySnpEffDirectoryStructure(final java.io.File dir) {
    File[] resourceDirFiles = dir.listFiles();
    assertThat(resourceDirFiles).hasSize(1);

    val snpeffDir = resourceDirFiles[0];
    assertThat(snpeffDir.getName()).isEqualTo(VERSION);

    val snpeffDbFile = new File(snpeffDir, "dcc-snpeff-" + VERSION + ".tar");
    assertFile(snpeffDbFile, 5120);

    // Verifyting uncompressed snpEff databases
    val motifDb = new File(snpeffDir, "motif.bin");
    assertFile(motifDb, 2);
    val nextProtDb = new File(snpeffDir, "nextProt.bin");
    assertFile(nextProtDb, 3);
    val pwmsDb = new File(snpeffDir, "pwms.bin");
    assertFile(pwmsDb, 4);
    val predictionDb = new File(snpeffDir, "snpEffectPredictor.bin");
    assertFile(predictionDb, 5);
  }

  private static void assertFile(File file, long expectedSize) {
    assertThat(file.exists()).isTrue();
    assertThat(file.length()).isEqualTo(expectedSize);
  }

}
