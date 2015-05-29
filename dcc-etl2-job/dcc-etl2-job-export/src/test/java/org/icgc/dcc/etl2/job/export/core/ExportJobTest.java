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
import static org.icgc.dcc.etl2.core.util.HadoopFileSystemUtils.getFilePaths;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;

import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.icgc.dcc.etl2.core.job.DefaultJobContext;
import org.icgc.dcc.etl2.core.job.JobContext;
import org.icgc.dcc.etl2.core.job.JobType;
import org.icgc.dcc.etl2.core.task.TaskExecutor;
import org.icgc.dcc.etl2.job.export.model.ExportTable;
import org.icgc.dcc.etl2.job.export.test.hbase.EmbeddedHBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.Table;
import com.google.common.util.concurrent.MoreExecutors;

@Slf4j
public class ExportJobTest {

  /**
   * 
   * Constants.
   */
  private static final String TEST_FIXTURES_DIR = "src/test/resources/fixtures";
  private static final String INPUT_DIR = TEST_FIXTURES_DIR + "/export_input";

  /**
   * Collaborators.
   */
  EmbeddedHBase hbase;

  /**
   * Class under test.
   */
  ExportJob job;

  /**
   * Configuration
   */
  Configuration config;
  Path hadoopWorkingDir;
  JavaSparkContext sparkContext;
  TaskExecutor taskExecutor;
  FileSystem fileSystem;
  File workingDir;

  /**
   * State.
   */
  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  @SneakyThrows
  @Before
  public void setUp() {
    this.hbase = new EmbeddedHBase();
    log.info("> Starting embedded HBase...");
    hbase.startUp();
    log.info("< Started embedded HBase");

    this.config = hbase.getConfig();

    this.workingDir = tmp.newFolder("working");
    this.hadoopWorkingDir = new Path("/", this.workingDir.getName());

    // FIXME: Needed since HBase context is configured after the base class's sparkContext and we need HDFS
    val sparkConf = new SparkConf().setAppName("test").setMaster("local");
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.set("spark.kryo.registrator", "org.icgc.dcc.etl2.core.util.CustomKryoRegistrator");
    sparkConf.set("spark.task.maxFailures", "0");
    this.sparkContext = new JavaSparkContext(sparkConf);

    val sparkConfig = sparkContext.hadoopConfiguration();
    setDefaultUri(sparkConfig, getDefaultUri(config));

    this.job = new ExportJob(config);

    this.fileSystem = FileSystem.get(this.config);
    val executor = MoreExecutors.sameThreadExecutor();
    this.taskExecutor = new TaskExecutor(executor, sparkContext, fileSystem);
  }

  @After
  public void shutDown() {
    sparkContext.stop();
    sparkContext = null;
    System.clearProperty("spark.master.port");
  }

  @Test
  @SneakyThrows
  public void testExportHFiles() {

    val inputDirectory = new File(INPUT_DIR);

    log.info("Using data from '{}'", inputDirectory.getAbsolutePath());

    copyFiles(new Path(inputDirectory.getAbsolutePath()), new Path("/", this.hadoopWorkingDir + "/export_input"));

    val jobContext = createJobContext(job.getType());
    job.execute(jobContext);

    val tableName = ExportTable.Clinical.name();
    val table = hbase.getTable(tableName);
    val rowCount = hbase.getRowCount(tableName);
    assertThat(rowCount).isEqualTo(15);

    // val get = new Get(Bytes.toBytes(1));
    // get.addFamily(ExportTables.DATA_CONTENT_FAMILY);
    // val result = table.get(get);
    // assertThat(result.isEmpty()).isFalse();
    //
    // byte[] expectedBytes = Bytes.toBytes("");
    //
    // byte[] family = ExportTables.DATA_CONTENT_FAMILY;
    // byte[] qualifier = ExportTables.META_TYPE_HEADER;
    // byte[] actualBytes = result.getValue(family, qualifier);
    //
    // assertThat(actualBytes).isEqualTo(expectedBytes);
  }

  private void copyFiles(Path source, Path target) throws IOException {
    fileSystem.copyFromLocalFile(source, target);
    // Verify
    val files = getFilePaths(fileSystem, target);
    for (val file : files) {
      log.info(file);
    }
  }

  @SuppressWarnings("unchecked")
  protected JobContext createJobContext(JobType type) {
    return new DefaultJobContext(type, "ICGC18", of(""), "/dev/null", hadoopWorkingDir.toString(), mock(Table.class),
        taskExecutor);
  }

}
