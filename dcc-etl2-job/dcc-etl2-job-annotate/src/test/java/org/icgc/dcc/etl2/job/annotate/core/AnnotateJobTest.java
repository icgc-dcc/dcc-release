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
package org.icgc.dcc.etl2.job.annotate.core;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.List;

import lombok.val;

import org.icgc.dcc.etl2.core.job.FileType;
import org.icgc.dcc.etl2.job.annotate.config.SnpEffProperties;
import org.icgc.dcc.etl2.test.job.AbstractJobTest;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class AnnotateJobTest extends AbstractJobTest {

  /**
   * Constants.
   */
  private static final String TEST_FIXTURES_DIR = "src/test/resources/fixtures";
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
    given(new File(INPUT_DIR));
  }

  @Test
  public void testExecute() {
    val jobContext = createJobContext(job.getType(), singletonList(PROJECT_NAME));
    job.execute(jobContext);

    val ssmResults = produces(PROJECT_NAME, FileType.SSM_S);
    assertThat(ssmResults).hasSize(4);
    verifyResults(ssmResults);

    val sgvResults = produces(PROJECT_NAME, FileType.SGV_S);
    assertThat(sgvResults).hasSize(9);
    verifyResults(sgvResults);
  }

  private static void verifyResults(List<ObjectNode> results) {
    for (val result : results) {
      assertResultFile(result);
    }
  }

  private static void assertResultFile(ObjectNode result) {
    assertThat(result.get("consequence_type").asText()).endsWith("_variant");
    assertThat(result.get("gene_affected").asText()).startsWith("ENSG");
    assertThat(result.get("transcript_affected").asText()).startsWith("ENST");
    assertThat(result.get("gene_build_version").asText()).isEqualTo("75");
    assertThat(result.get("observation_id").asText()).isEqualTo("zzz123");

    assertThat(result.get("aa_change").isNull()).isTrue();
    assertThat(result.get("cds_change").isNull()).isTrue();
    assertThat(result.get("protein_domain_affected").isNull()).isTrue();
    assertThat(result.get("note").isNull()).isTrue();
  }

  private static SnpEffProperties createSnpEffProperties() {
    val result = new SnpEffProperties();
    result.setDatabaseVersion("3.6c-GRCh37.75");
    result.setGeneBuildVersion("75");
    result.setMaxFileSizeMb(512);
    result.setReferenceGenomeVersion("GRCh37.75.v1");
    result.setResourceDir(new File("/tmp/dcc-etl2"));
    result.setResourceUrl("https://seqwaremaven.oicr.on.ca/artifactory/simple/dcc-dependencies/org/icgc/dcc");
    result.setVersion("3.6c");

    return result;
  }

}
