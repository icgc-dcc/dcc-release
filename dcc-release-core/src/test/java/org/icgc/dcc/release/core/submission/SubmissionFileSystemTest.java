/*
 * Copyright (c) 2017 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.release.core.submission;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.release.test.util.SubmissionFiles;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Table;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SubmissionFileSystemTest {

  protected FileSystem fileSystem;
  SubmissionFileSystem submissionFileSystem;
  List<SubmissionFileSchema> metadata;

  private static final List<String> PROJECTS = ImmutableList.of("PROJ-01", "PROJ-02", "PROJ-03", "PROJ-0A", "PROJ-0B");

  protected static final String TEST_FIXTURES_DIR = "src/test/resources/fixtures";

  protected File submissionDir;
  protected File pcawgDir;

  protected File resolveFixturePath(String fileName) {
    return new File(TEST_FIXTURES_DIR + "/" + fileName);
  }

  @Rule
  public TemporaryFolder workingFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    this.fileSystem = FileSystem.getLocal(new Configuration());
    this.submissionFileSystem = new SubmissionFileSystem(fileSystem);
    this.metadata = SubmissionFiles.getMetadata();

    File workDir = workingFolder.getRoot();
    log.info("Working dir: " + workDir.getAbsolutePath());

    // note: permitted file names for ssm_m/ssm_p are:
    // http://docs.icgc.org/dictionary/viewer/#?viewMode=details&dataType=ssm_m
    // http://docs.icgc.org/dictionary/viewer/#?viewMode=details&dataType=ssm_p
    // ^ssm_m(\.[a-zA-Z0-9]+)?\.txt(?:\.gz)?$
    // ^ssm_p(\.[a-zA-Z0-9]+)?\.txt(?:\.gz)?$

    FileUtils.copyDirectoryToDirectory(resolveFixturePath("submission"), workDir);
    FileUtils.copyDirectoryToDirectory(resolveFixturePath("pcawg"), workDir);

    submissionDir = Paths.get(workDir.getAbsolutePath(), "submission").toFile();
    pcawgDir = Paths.get(workDir.getAbsolutePath(), "pcawg").toFile();
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void test_single_ingest_path() {
    val result = submissionFileSystem.getFiles(submissionDir.getAbsolutePath(), PROJECTS, metadata);
    assertThat(countFiles(result)).isEqualTo(15);
  }

  @Test
  public void test_multiple_ingest_path() {
    val result = submissionFileSystem
        .getFiles(Arrays.asList(submissionDir.getAbsolutePath(), pcawgDir.getAbsolutePath()), PROJECTS, metadata);
    assertThat(countFiles(result)).isEqualTo(21);
  }

  private int countFiles(Table<String, String, List<Path>> table) {
    int result = 0;
    for (val cell : table.cellSet()) {
      val rowKey = cell.getRowKey();
      val colKey = cell.getColumnKey();
      List<Path> files = table.get(rowKey, colKey);
      result += files.size();
    }
    return result;
  }

}
