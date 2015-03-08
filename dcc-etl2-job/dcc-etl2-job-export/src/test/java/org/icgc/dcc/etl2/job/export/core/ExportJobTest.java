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
package org.icgc.dcc.etl2.job.export.core;

import static com.google.common.collect.ImmutableList.of;
import static org.apache.hadoop.fs.FileSystem.getDefaultUri;
import static org.apache.hadoop.fs.FileSystem.setDefaultUri;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.io.IOException;

import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.util.Bytes;
import org.icgc.dcc.etl2.core.job.DefaultJobContext;
import org.icgc.dcc.etl2.core.job.JobContext;
import org.icgc.dcc.etl2.core.job.JobType;
import org.icgc.dcc.etl2.job.export.model.ExportTable;
import org.icgc.dcc.etl2.job.export.model.ExportTables;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Table;

@Slf4j
public class ExportJobTest extends BaseExportJobTest {

  Configuration config;
  Path hadoopWorkingDir;

  @Override
  @Before
  public void setUp() {
    super.setUp();

    this.config = hbase.getConfig();
    this.hadoopWorkingDir = new Path("/", super.workingDir.getName());

    // FIXME: Needed since HBase context is configured after the base class's sparkContext and we need HDFS
    val sparkConfig = sparkContext.hadoopConfiguration();
    setDefaultUri(sparkConfig, getDefaultUri(config));
  }

  @Override
  @After
  public void shutDown() {
    super.shutDown();
  }

  @Test
  @SneakyThrows
  public void testExportHFiles() {
    // Test data
    val id = "DO1";
    val row = row("{id: '" + id + "', data: 1}");
    val rows = of(row);

    // Setup
    val executor = createTaskExecutor();
    val job = new ExportJob(executor, config);
    val jobContext = createJobContext(job.getType());

    // Simulate exporter input dynamically
    given(inputFile()
        .fileType("export_input")
        .rows(rows));

    copyFiles();

    // Exercise
    log.info("Testing...");
    job.execute(jobContext);

    // Verify
    val tableName = ExportTable.export_output.name();
    val table = hbase.getTable(tableName);
    val rowCount = hbase.getRowCount(tableName);
    assertThat(rowCount).isEqualTo(1);

    val get = new Get(Bytes.toBytes(id));
    get.addFamily(ExportTables.META_TYPE_INFO_FAMILY);
    val result = table.get(get);
    assertThat(result.isEmpty()).isFalse();

    byte[] expectedBytes = Bytes.toBytes(row.toString());

    byte[] family = ExportTables.META_TYPE_INFO_FAMILY;
    byte[] qualifier = ExportTables.META_TYPE_HEADER;
    byte[] actualBytes = result.getValue(family, qualifier);

    assertThat(actualBytes).isEqualTo(expectedBytes);
  }

  private void copyFiles() throws IOException {
    // TODO: Generalize
    val fileSystem = FileSystem.get(config);
    fileSystem.copyFromLocalFile(new Path(super.workingDir.getAbsolutePath()), hadoopWorkingDir);
  }

  @SuppressWarnings("unchecked")
  private JobContext createJobContext(JobType type) {
    return new DefaultJobContext(type, "ICGC18", of(""), "/dev/null", hadoopWorkingDir.toString(), mock(Table.class));
  }

}