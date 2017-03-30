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
package org.icgc.dcc.release.job.stage.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.core.model.FieldNames.PROJECT_ID;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames;
import org.icgc.dcc.release.core.job.DefaultJobContext;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.job.JobContext;
import org.icgc.dcc.release.core.job.JobType;
import org.icgc.dcc.release.core.submission.SubmissionFileSchema;
import org.icgc.dcc.release.core.submission.SubmissionFileSystem;
import org.icgc.dcc.release.core.util.LazyTable;
import org.icgc.dcc.release.test.job.AbstractJobTest;
import org.icgc.dcc.release.test.util.SubmissionFiles;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Table;

import lombok.val;

public class StageJobTest extends AbstractJobTest {

  protected static final String PCAWG_TEST_FIXTURES_DIR = "src/test/resources/pcawg_fixtures";
  private static final List<String> PROJECTS = ImmutableList.of("BOCA-UK", "BRCA-UK", "BTCA-SG", "EOPC-DE", "LAML-KR",
      "LICA-FR", "LIRI-JP", "OV-AU", "PACA-AU", "PAEN-AU", "PRAD-CA", "PRAD-UK", "RECA-EU");

  SubmissionFileSystem submissionFileSystem;

  /**
   * Class under test.
   */
  StageJob job;

  @Override
  @Before
  public void setUp() {
    super.setUp();
    this.job = new StageJob(SubmissionFiles.getSchemas());
    this.submissionFileSystem = new SubmissionFileSystem(fileSystem);
  }

  @Test
  public void testExecute() {
    val jobContext = createJobContext();
    job.execute(jobContext);

    assertBocaUK();
  }

  private void assertSsm_p_nulls() {
    val ssms = produces("BOCA-UK", FileType.SSM_P);
    for (val ssm : ssms) {

      assertThat(ssm.path(SubmissionFieldNames.SUBMISSION_OBSERVATION_MUTATED_FROM_ALLELE).isMissingNode()).isFalse();
      assertThat(ssm.path(SubmissionFieldNames.SUBMISSION_OBSERVATION_CONTROL_GENOTYPE).isMissingNode()).isFalse();
      assertThat(ssm.path(SubmissionFieldNames.SUBMISSION_OBSERVATION_TUMOUR_GENOTYPE).isMissingNode()).isFalse();
    }

  }

  private void assertSsm_p() {
    val ssms = produces("BOCA-UK", FileType.SSM_P);
    assertThat(ssms).hasSize(5007); // * from submission fixtures (3 + 1 + 4 + 4999)

    for (val ssm : ssms) {
      assertThat(ssm.get(PROJECT_ID).textValue()).isEqualTo("BOCA-UK");
      assertThat(ssm.path(SubmissionFieldNames.SUBMISSION_OBSERVATION_MUTATED_FROM_ALLELE).isMissingNode()).isFalse();
      assertThat(ssm.path(SubmissionFieldNames.SUBMISSION_OBSERVATION_CONTROL_GENOTYPE).isMissingNode()).isFalse();
      assertThat(ssm.path(SubmissionFieldNames.SUBMISSION_OBSERVATION_TUMOUR_GENOTYPE).isMissingNode()).isFalse();
    }
  }

  private void assertSsm_m() {
    val ssms = produces("BOCA-UK", FileType.SSM_M);
    assertThat(ssms).hasSize(131); // * from submission fixtures (1 + 1 + 1 + 128)
  }

  private void assertBocaUK() {
    assertControlledFields();
    assertSsm_p();
    assertSsm_m();

    assertFlagM();
    assertFlagP();
  }

  private void assertFlagM() {
    val ssms = produces("BOCA-UK", FileType.SSM_M);

    for (val ssm : ssms) {
      val node = ssm.get("pcawg_flag");
      assertThat(node).isNotNull(); // JsonNode is present (but value is null) - if pcawg_flag was actually missing,
                                    // node itself would be null
    }
    val counts = countFlags(ssms);
    assertThat(counts.getLeft()).isEqualTo(3);
    assertThat(counts.getRight()).isEqualTo(128);
  }

  private void assertFlagP() {
    val ssms = produces("BOCA-UK", FileType.SSM_P);

    for (val ssm : ssms) {
      val node = ssm.get("pcawg_flag");
      assertThat(node).isNotNull(); // JsonNode is present (but value is null) - if pcawg_flag was actually missing,
                                    // node itself would be null
    }
    val counts = countFlags(ssms);

    assertThat(counts.getLeft()).isEqualTo(8);
    assertThat(counts.getRight()).isEqualTo(4999);
  }

  private void assertControlledFields() {
    val donors = produces("BOCA-UK", FileType.DONOR);
    for (val donor : donors) {
      assertThat(donor.path(" donor_region_of_residence").isMissingNode()).isTrue();
      assertThat(donor.path("donor_notes").isMissingNode()).isTrue();
    }
  }

  private JobContext createJobContext() {
    Table<String, String, List<Path>> submissionFiles = resolveSubmissionFiles();

    val result = new DefaultJobContext(JobType.STAGE, RELEASE_VERSION, PROJECTS, Arrays.asList(TEST_FIXTURES_DIR),
        workingDir.toString(), submissionFiles, taskExecutor, false);
    return result;

  }

  private Table<String, String, List<Path>> resolveSubmissionFiles() {
    return new LazyTable<String, String, List<Path>>(() -> {
      List<SubmissionFileSchema> metadata = SubmissionFiles.getMetadata();

      return submissionFileSystem.getFiles(Arrays.asList(TEST_FIXTURES_DIR, PCAWG_TEST_FIXTURES_DIR), PROJECTS,
          metadata);
    });
  }

  private Pair<Integer, Integer> countFlags(List<ObjectNode> nodes) {
    int submissionCount = 0;
    int pcawgCount = 0;

    for (val ssm : nodes) {
      val node = ssm.get("pcawg_flag");
      if (node.isNull()) {
        submissionCount += 1;
      } else if (node.textValue().equals("true")) {
        pcawgCount += 1;
      }
    }
    return Pair.of(submissionCount, pcawgCount);
  }

}
