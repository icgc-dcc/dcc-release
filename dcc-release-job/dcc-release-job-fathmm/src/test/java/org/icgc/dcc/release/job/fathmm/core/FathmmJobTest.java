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
package org.icgc.dcc.release.job.fathmm.core;

import static org.icgc.dcc.release.job.fathmm.util.PostgresTests.initDb;

import java.io.File;

import lombok.val;

import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.job.fathmm.util.PostgresTests;
import org.icgc.dcc.release.test.job.AbstractJobTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.ImmutableList;
import com.opentable.db.postgres.junit.EmbeddedPostgresRules;
import com.opentable.db.postgres.junit.SingleInstancePostgresRule;

public class FathmmJobTest extends AbstractJobTest {

  private static final String PROJECT_NAME = "TEST-DCC";

  /**
   * Class under test.
   */
  FathmmJob job;

  @Rule
  public SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance();

  @Before
  public void setUpFathmmJobTest() throws Exception {
    val embeddedPostgres = pg.getEmbeddedPostgres();
    val dataSource = embeddedPostgres.getPostgresDatabase();
    initDb(dataSource);

    this.job = new FathmmJob();
    val jdbcUrl = PostgresTests.getJdbcUrl(embeddedPostgres);
    ReflectionTestUtils.setField(job, "jdbcUrl", jdbcUrl);
  }

  @After
  public void tearDown() throws Exception {
    pg.getEmbeddedPostgres().close();
  }

  @Test
  public void executeTest() {
    given(new File(TEST_FIXTURES_DIR));
    val jobContext = createJobContext(job.getType(), ImmutableList.of(PROJECT_NAME));
    job.execute(jobContext);

    verifyResult(PROJECT_NAME, FileType.OBSERVATION_FATHMM);
  }

}
