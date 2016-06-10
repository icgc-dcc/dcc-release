/*
 * Copyright (c) 2015 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.release.job.annotate.core;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.List;

import lombok.val;

import org.icgc.dcc.release.core.config.SnpEffProperties;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.test.job.AbstractJobTest;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class AnnotateJobTest extends AbstractJobTest {

  /**
   * Constants.
   */
  private static final String INPUT_DIR = TEST_FIXTURES_DIR + "/staging";
  private static final String PROJECT_NAME = "BRCA-UK";

  /**
   * Class under test.
   */
  AnnotateJob job;

  @Before
  @Override
  public void setUp() {
    super.setUp();
    this.job = new AnnotateJob(createSnpEffProperties());
  }

  @Test
  public void testExecute() {
    given(new File(INPUT_DIR));
    val jobContext = createJobContext(job.getType(), singletonList(PROJECT_NAME));
    job.execute(jobContext);

    val ssmResults = produces(PROJECT_NAME, FileType.SSM_S);
    assertThat(ssmResults).hasSize(4);
    verifyResults(ssmResults, FileType.SSM);

    val sgvResults = produces(PROJECT_NAME, FileType.SGV_S);
    assertThat(sgvResults).hasSize(9);
    verifyResults(sgvResults, FileType.SGV);
  }

  private static void verifyResults(List<ObjectNode> results, FileType fileType) {
    for (val result : results) {
      assertResultFile(result, fileType);
    }
  }

  private static void assertResultFile(ObjectNode result, FileType fileType) {
    assertThat(result.get("consequence_type").asText()).endsWith("_variant");
    assertThat(result.get("gene_affected").asText()).startsWith("ENSG");
    assertThat(result.get("transcript_affected").asText()).startsWith("ENST");
    assertThat(result.get("gene_build_version").asText()).isEqualTo("75");
    assertThat(result.get("observation_id").asText()).isEqualTo("zzz123");

    if (fileType == FileType.SGV) {
      assertThat(result.get("aa_change").isNull()).isTrue();
      assertThat(result.get("cds_change").isNull()).isTrue();
    } else {
      assertThat(result.get("aa_mutation").isNull()).isTrue();
      assertThat(result.get("cds_mutation").isNull()).isTrue();
    }

    assertThat(result.get("protein_domain_affected").isNull()).isTrue();
    assertThat(result.get("note").isNull()).isTrue();
  }

  private static SnpEffProperties createSnpEffProperties() {
    val result = new SnpEffProperties();
    result.setDatabaseVersion("3.6c-GRCh37.75");
    result.setGeneBuildVersion("75");
    result.setMaxFileSizeMb(512);
    result.setReferenceGenomeVersion("GRCh37.75.v1");
    result.setResourceDir(new File("/tmp/dcc-release"));
    result.setResourceUrl("https://artifacts.oicr.on.ca/artifactory/simple/dcc-dependencies/org/icgc/dcc");
    result.setVersion("3.6c");

    return result;
  }

}
