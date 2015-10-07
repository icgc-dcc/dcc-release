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

import java.io.File;
import java.util.List;

import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.core.meta.FileCodeListsResolver;
import org.icgc.dcc.common.core.meta.FileDictionaryResolver;
import org.icgc.dcc.common.core.model.FieldNames;
import org.icgc.dcc.release.core.job.DefaultJobContext;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.job.JobContext;
import org.icgc.dcc.release.core.job.JobType;
import org.icgc.dcc.release.core.submission.SubmissionFileSchema;
import org.icgc.dcc.release.core.submission.SubmissionFileSchemas;
import org.icgc.dcc.release.core.submission.SubmissionFileSystem;
import org.icgc.dcc.release.core.submission.SubmissionMetadataService;
import org.icgc.dcc.release.core.util.LazyTable;
import org.icgc.dcc.release.test.job.AbstractJobTest;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Table;

@Slf4j
public class StageJobTest extends AbstractJobTest {

  private static final String DICTIONARY_FILE = TEST_FIXTURES_DIR + "/dictionary.json.gz";
  private static final String CODE_LISTS_FILE = TEST_FIXTURES_DIR + "/codelists.json.gz";
  private static final List<String> PROJECTS = ImmutableList.of("PROJ-01", "PROJ-02", "PROJ-03");

  SubmissionFileSystem submissionFileSystem;

  /**
   * Class under test.
   */
  StageJob job;

  @Override
  @Before
  public void setUp() {
    super.setUp();
    this.job = new StageJob(getSchemas());
    this.submissionFileSystem = new SubmissionFileSystem(fileSystem);
  }

  @Test
  public void testExecute() {
    // copySubmissionFiles();
    val jobContext = createJobContext();
    job.execute(jobContext);

    assertProj1();
  }

  private void assertProj1() {
    val results = produces("PROJ-01", FileType.SSM_P);
    assertThat(results).hasSize(8);
    log.info("{}", results.get(0));
    assertThat(results.get(0).get(FieldNames.PROJECT_ID).textValue()).isEqualTo("PROJ-01");
  }

  private JobContext createJobContext() {
    return new DefaultJobContext(JobType.STAGE, RELEASE_VERSION, PROJECTS, TEST_FIXTURES_DIR,
        workingDir.toString(), resolveSubmissionFiles(), taskExecutor);
  }

  private Table<String, String, List<Path>> resolveSubmissionFiles() {
    return new LazyTable<String, String, List<Path>>(() -> {
      List<SubmissionFileSchema> metadata = getMetadata();

      return submissionFileSystem.getFiles(TEST_FIXTURES_DIR, PROJECTS,
          metadata);
    });
  }

  private SubmissionFileSchemas getSchemas() {
    return new SubmissionFileSchemas(getMetadata());
  }

  private List<SubmissionFileSchema> getMetadata() {
    return new SubmissionMetadataService(
        new FileDictionaryResolver(DICTIONARY_FILE),
        new FileCodeListsResolver(CODE_LISTS_FILE))
        .getMetadata();
  }

  @SneakyThrows
  private void copySubmissionFiles() {
    FileUtils.copyDirectory(new File(TEST_FIXTURES_DIR), workingDir);
  }

}
