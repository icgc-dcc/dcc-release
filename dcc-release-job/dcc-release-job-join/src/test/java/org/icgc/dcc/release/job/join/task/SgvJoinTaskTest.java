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
package org.icgc.dcc.release.job.join.task;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.job.JobType;
import org.icgc.dcc.release.core.task.TaskContext;
import org.icgc.dcc.release.test.job.AbstractJobTest;
import org.junit.Before;
import org.junit.Test;

@Slf4j
public class SgvJoinTaskTest extends AbstractJobTest {

  private static final String PROJECT_NAME = "BRCA-UK";

  SgvJoinTask task;

  TaskContext taskContext;

  @Before
  @Override
  public void setUp() {
    super.setUp();
    given(new File(INPUT_TEST_FIXTURES_DIR));
    taskContext = createTaskContext(JobType.JOIN, PROJECT_NAME);
    val sparkContext = taskContext.getSparkContext();

    task = new SgvJoinTask(sparkContext.broadcast(emptyMap()), sparkContext.broadcast(emptyMap()));
  }

  @Test
  public void testExecute() throws Exception {
    task.execute(createTaskContext(JobType.JOIN, PROJECT_NAME));
    val sgv = produces(PROJECT_NAME, FileType.SGV);
    log.info("SGV: {}", sgv);
    assertThat(sgv).isNotEmpty();
  }

}
