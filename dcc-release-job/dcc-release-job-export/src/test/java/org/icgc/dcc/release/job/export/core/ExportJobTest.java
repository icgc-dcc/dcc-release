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

import static org.apache.hadoop.fs.FileSystem.getDefaultUri;
import static org.apache.hadoop.fs.FileSystem.setDefaultUri;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.release.core.util.HadoopFileSystemUtils.getFilePaths;
import static org.icgc.dcc.release.job.export.model.ExportTables.*;
import static org.icgc.dcc.release.job.export.model.type.Constants.DONOR_ID;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.icgc.dcc.release.core.job.DefaultJobContext;
import org.icgc.dcc.release.core.job.JobContext;
import org.icgc.dcc.release.core.job.JobType;
import org.icgc.dcc.release.core.task.TaskExecutor;
import org.icgc.dcc.release.job.export.core.ExportJob;
import org.icgc.dcc.release.job.export.model.ExportTable;
import org.icgc.dcc.release.job.export.test.hbase.EmbeddedHBase;
import org.icgc.dcc.release.job.export.util.HTableManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.*;
import com.google.common.util.concurrent.MoreExecutors;

@Slf4j
public class ExportJobTest {

  /**
   * 
   * Constants.
   */
  private static final String TEST_FIXTURES_DIR = "src/test/resources/fixtures";
  private static final String INPUT_DIR = TEST_FIXTURES_DIR + "/export_input";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Collaborators.
   */
  private EmbeddedHBase hbase;

  /**
   * Class under test.
   */
  private ExportJob job;

  /**
   * Configuration
   */
  private Configuration config;
  private Path hadoopWorkingDir;
  private JavaSparkContext sparkContext;
  private TaskExecutor taskExecutor;
  private FileSystem fileSystem;
  private File workingDir;

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
    sparkConf.set("spark.kryo.registrator", "org.icgc.dcc.release.core.util.CustomKryoRegistrator");
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

  @SuppressWarnings("deprecation")
  @Test
  @SneakyThrows
  public void testExportHFiles() {
    val inputDirectory = new File(INPUT_DIR);
    copyFiles(new Path(inputDirectory.getAbsolutePath()), new Path("/", this.hadoopWorkingDir + "/export_input"));
    log.info("Using data from '{}'", inputDirectory.getAbsolutePath());

    val projectNames = ImmutableList.of("All-US", "EOPC-DE", "PRAD-CA");
    val releaseName = "ICGC19";
    val jobContext = createJobContext(job.getType(), releaseName, projectNames, hadoopWorkingDir.toString());

    job.execute(jobContext);

    val tableName = getTableName(ExportTable.CLINICAL.name, releaseName);
    val staticOutputFile = getStaticFileOutput(tableName);
    List<ObjectNode> staticOutput = sparkContext.textFile(staticOutputFile)
        .collect()
        .stream()
        .map(row -> readJson(row))
        .collect(Collectors.toList());

    assertThat(staticOutput.size()).isEqualTo(15);
    val rowCount = hbase.getRowCount(tableName);
    assertThat(rowCount).isEqualTo(15);

    ListMultimap<Integer, ObjectNode> donors = collectDonors(staticOutput);
    Map<String, Integer> values = collectValueCount(staticOutput);

    val scanner = hbase.scanTable(tableName, DATA_CONTENT_FAMILY);
    for (Result result = scanner.next(); (result != null); result = scanner.next()) {
      for (KeyValue keyValue : result.list()) {
        val family = keyValue.getFamily();
        val qualifier = keyValue.getQualifier();
        val cell = keyValue.getValue();
        log.info("Family : '{}', Qualifier : '{}', Value: '{}'", Bytes.toString(family), qualifier,
            Bytes.toString(cell));

        assertThat(family).isEqualTo(DATA_CONTENT_FAMILY);

        val rowByte = keyValue.getRow();
        val compositeKey = HTableManager.decodeRowKey(rowByte);
        val donorId = compositeKey.getDonorId();
        List<ObjectNode> rows = donors.get(donorId);
        assertThat(rows).isNotNull();
        assertThat(rows.size()).isGreaterThan(0);

        val cellValue = Bytes.toString(cell);
        val count = values.get(cellValue);

        assertThat(count).isNotNull();
        assertThat(count).isGreaterThan(0);

        values.put(cellValue, count - 1);

      }
    }

    // Make sure all values have been found.
    for (val key : values.keySet()) {
      assertThat(values.get(key)).isEqualTo(0);
    }

  }

  private void copyFiles(Path source, Path target) throws IOException {
    fileSystem.copyFromLocalFile(source, target);
    // TODO Remove verification
    val files = getFilePaths(fileSystem, target);
    files.forEach(log::info);
  }

  @SuppressWarnings("unchecked")
  private JobContext createJobContext(JobType type, String release, List<String> projects, String workingDir) {
    return new DefaultJobContext(type, release, projects, "/dev/null", workingDir, mock(Table.class),
        taskExecutor);
  }

  @SneakyThrows
  private ObjectNode readJson(String row) {
    return (ObjectNode) MAPPER.readTree(row);
  }

  private Map<String, Integer> collectValueCount(List<ObjectNode> staticOutput) {
    Map<String, Integer> values = Maps.newHashMap();
    for (val row : staticOutput) {
      val fields = row.fieldNames();
      while (fields.hasNext()) {
        val fieldName = fields.next();
        val value = row.get(fieldName);
        if (!(value == null || value.isNull())) {
          val count = values.get(value.asText());
          if (count == null) {
            values.put(value.asText(), 1);
          } else {
            values.put(value.asText(), count + 1);
          }
        }
      }
    }

    return values;
  }

  private ListMultimap<Integer, ObjectNode> collectDonors(List<ObjectNode> staticOutput) {
    ListMultimap<Integer, ObjectNode> donors = ArrayListMultimap.create();
    for (val row : staticOutput) {
      val donorId = row.get(DONOR_ID).asInt();
      donors.put(donorId, row);
    }

    return donors;
  }

}
