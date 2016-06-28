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
package org.icgc.dcc.release.job.export.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.core.util.Joiners.PATH;
import static org.icgc.dcc.release.job.export.io.GzipRowWriter.GZIP_FILES_DIR;

import java.io.File;

import lombok.val;

import org.icgc.dcc.common.test.file.FileTests;
import org.icgc.dcc.release.job.export.config.ExportProperties;
import org.icgc.dcc.release.test.job.AbstractJobTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class ExportJobTest extends AbstractJobTest {

  private static final String PROJECT1 = "TST1-CA";
  private static final String PROJECT2 = "TST2-CA";

  /**
   * Class under test.
   */
  ExportJob job;
  ExportProperties exportProperties;

  @Override
  @Before
  public void setUp() {
    super.setUp();
    exportProperties = new ExportProperties()
        .setSqlCompressionCodec("gzip");
    this.job = new ExportJob(exportProperties, fileSystem, sparkContext);
  }

  @Test
  @Ignore("Finish ExportJob#createTaskWithParquet() to run this test")
  public void testExecute_parquet() {
    exportProperties.setExportParquet(true);
    given(new File(INPUT_TEST_FIXTURES_DIR));

    val jobContext = createJobContext(job.getType(), ImmutableList.of(PROJECT1, PROJECT2));
    job.execute(jobContext);

    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext);
    val inputPath = new File(workingDir, "export/donor").getAbsolutePath();

    val input = sqlContext.read().parquet(inputPath);
    input.show();

    assertThat(input.count()).isEqualTo(4L);
    assertThat(input.groupBy("_donor_id").count().count()).isEqualTo(4L);
  }

  @Test
  public void testExecute_gzip() {
    given(new File(INPUT_TEST_FIXTURES_DIR));

    val jobContext = createJobContext(job.getType(), ImmutableList.of(PROJECT1, PROJECT2));
    job.execute(jobContext);

    val expectedDir = new File(OUTPUT_TEST_FIXTURES_DIR, GZIP_FILES_DIR);
    val actualDir = new File(PATH.join(workingDir.getAbsolutePath(), "export", GZIP_FILES_DIR));
    FileTests.compareDirectories(expectedDir, actualDir);
  }

}
