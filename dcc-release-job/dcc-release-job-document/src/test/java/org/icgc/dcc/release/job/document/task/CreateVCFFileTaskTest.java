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
package org.icgc.dcc.release.job.document.task;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.release.job.index.utils.TestUtils.createSnpEffProperties;

import java.io.File;

import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.core.io.Files2;
import org.icgc.dcc.release.core.job.JobType;
import org.icgc.dcc.release.core.task.Task;
import org.icgc.dcc.release.job.document.function.SaveVCFRecords;
import org.icgc.dcc.release.test.job.AbstractJobTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Slf4j
public class CreateVCFFileTaskTest extends AbstractJobTest {

  private static final String COMPRESSED_INPUT = TEST_FIXTURES_DIR + "/compressed/input";

  Task task = new CreateVCFFileTask(createSnpEffProperties());

  @Before
  @Override
  public void setUp() {
    super.setUp();
  }

  @Test
  public void testExecute() throws Exception {
    given(new File(INPUT_TEST_FIXTURES_DIR));
    task.execute(createTaskContext(JobType.SUMMARIZE));
    verifyOutput();
  }

  @Test
  public void testExecute_compressed() throws Exception {
    given(new File(COMPRESSED_INPUT));
    task.execute(createCompressedTaskContext(JobType.SUMMARIZE));
    verifyOutput();
  }

  private void verifyOutput() {
    val vcfFile = resolveVcfFile();
    printFile(vcfFile);
    assertThat(vcfFile.exists()).isTrue();
    assertThat(vcfFile.length()).isGreaterThan(1L);
  }

  @SneakyThrows
  private static void printFile(File vcfFile) {
    val reader = Files2.getCompressionAgnosticBufferedReader(vcfFile.getAbsolutePath());
    String line = null;
    while ((line = reader.readLine()) != null) {
      log.info(line);
    }
  }

  private File resolveVcfFile() {
    return new File(workingDir, SaveVCFRecords.VCF_FILE_NAME);
  }

}
