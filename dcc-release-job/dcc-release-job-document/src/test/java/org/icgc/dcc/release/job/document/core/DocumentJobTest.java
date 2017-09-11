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
package org.icgc.dcc.release.job.document.core;

import static org.icgc.dcc.release.job.index.utils.TestUtils.createSnpEffProperties;

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;
import lombok.val;

import org.icgc.dcc.release.core.config.SnpEffProperties;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.job.document.config.DocumentProperties;
import org.icgc.dcc.release.test.job.AbstractJobTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class DocumentJobTest extends AbstractJobTest {

  private static final String PROJECT = "BRCA-UK";

  private SnpEffProperties snpEffProperties;

  /**
   * Class under test.
   */
  DocumentJob job;

  @Override
  @Before
  public void setUp() {
    super.setUp();
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    try {
      snpEffProperties = mapper.treeToValue(mapper.readTree(DocumentJobTest.class.getResourceAsStream("/application.yml")).get("snpeff"), SnpEffProperties.class);

      val properties = new DocumentProperties();
      this.job = new DocumentJob(properties, snpEffProperties, sparkContext);
    } catch (IOException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }

  }

  @Test
  public void testExecute() {
    given(new File(INPUT_TEST_FIXTURES_DIR));
    job.execute(createJobContext(job.getType(), ImmutableList.of(PROJECT)));

    verifyResult(FileType.DRUG_TEXT_DOCUMENT);
    verifyResult(FileType.DRUG_CENTRIC_DOCUMENT);
    verifyResult(FileType.DIAGRAM_DOCUMENT);
    verifyResult(PROJECT, FileType.OBSERVATION_CENTRIC_DOCUMENT);
    verifyResult(FileType.PROJECT_DOCUMENT);
    verifyResult(FileType.PROJECT_TEXT_DOCUMENT);

    verifyResult(FileType.GENE_SET_DOCUMENT);
    verifyResult(FileType.GENE_SET_TEXT_DOCUMENT);

    verifyResult(FileType.GENE_DOCUMENT);
    verifyResult(FileType.GENE_TEXT_DOCUMENT);
    verifyResult(FileType.GENE_CENTRIC_DOCUMENT);

    verifyResult(FileType.MUTATION_CENTRIC_DOCUMENT);
    verifyResult(FileType.MUTATION_TEXT_DOCUMENT);

    verifyResult(FileType.RELEASE_DOCUMENT);

    verifyResult(PROJECT, FileType.DONOR_CENTRIC_DOCUMENT);
    verifyResult(PROJECT, FileType.DONOR_TEXT_DOCUMENT);
    verifyResult(PROJECT, FileType.DONOR_DOCUMENT);
  }

}
