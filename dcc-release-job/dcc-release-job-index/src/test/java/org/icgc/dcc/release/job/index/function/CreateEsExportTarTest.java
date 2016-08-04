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

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.release.core.document.DocumentType.DONOR_TYPE;
import static org.icgc.dcc.release.core.util.JacksonFactory.MAPPER;
import static org.icgc.dcc.release.test.util.TestJsonNodes.$;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import lombok.Cleanup;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.icgc.dcc.common.core.util.Joiners;
import org.icgc.dcc.common.hadoop.fs.FileSystems;
import org.icgc.dcc.release.core.document.Document;
import org.icgc.dcc.release.core.util.Configurations;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;

@Slf4j
public class CreateEsExportTarTest {

  private static final Map<String, String> FS_SETTINGS = getLocalFsSettigns();
  private static final Map<Integer, String> TAR_INDEX_TO_FILE_NAME = ImmutableMap.of(
      0, "icgc21/_settings",
      1, "icgc/donor/_mapping",
      2, "/icgc21/donor/DO1");

  CreateEsExportTar exportTarFunction;

  private File workingDir;

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  @Before
  public void setUp() {
    this.workingDir = tmp.newFolder("working");
    exportTarFunction =
        new CreateEsExportTar("icgc21", workingDir.getAbsolutePath(), DONOR_TYPE.getName(), FS_SETTINGS);
  }

  /**
   * @return
   */
  private static Map<String, String> getLocalFsSettigns() {
    val fs = FileSystems.getDefaultLocalFileSystem();

    return Configurations.getSettings(fs.getConf());
  }

  @Test
  public void testCall() throws Exception {
    val docSource = $("{_donor_id:'DO1'}");
    val doc = new Document(DONOR_TYPE, "DO1", docSource);
    exportTarFunction.call(Collections.singleton(doc).iterator());

    @Cleanup
    val tarIn = getTarReader();
    TarArchiveEntry tarEntry = null;
    int documentIndex = 0;

    // Read all the tar entries
    while ((tarEntry = tarIn.getNextTarEntry()) != null) {
      val fileName = tarEntry.getName();
      log.info("Entry name: {}", fileName);

      // Verify entries are added in expected order
      val expectedName = TAR_INDEX_TO_FILE_NAME.get(documentIndex++);
      assertThat(fileName.equals(expectedName));

      // And are not empty
      assertThat(tarEntry.getSize()).isGreaterThan(0);

      // Verify contents of the donor document
      if (fileName.equals("icgc21/donor/DO1")) {
        val donorSize = tarEntry.getSize();
        byte[] donorBytes = new byte[(int) donorSize];
        tarIn.read(donorBytes);
        val donorObject = MAPPER.readValue(donorBytes, ObjectNode.class);
        log.info("{}", donorObject);
        assertThat(donorObject).isEqualTo(docSource);
      }
    }
  }

  private TarArchiveInputStream getTarReader() throws IOException {
    val inputFileName = Joiners.PATH.join(workingDir.getAbsolutePath(), "es_export", "icgc21_donor.tar.gz");
    val gzipIn = new GZIPInputStream(new FileInputStream(new File(inputFileName)));

    return new TarArchiveInputStream(gzipIn);
  }

}
