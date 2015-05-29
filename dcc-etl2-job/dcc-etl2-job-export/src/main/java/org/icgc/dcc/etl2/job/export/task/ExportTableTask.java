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
package org.icgc.dcc.etl2.job.export.task;

import static org.icgc.dcc.common.core.util.Joiners.COMMA;
import static org.icgc.dcc.etl2.core.util.HadoopFileSystemUtils.getFilePaths;
import static org.icgc.dcc.etl2.core.util.HadoopFileSystemUtils.walkPath;
import static org.icgc.dcc.etl2.core.util.Stopwatches.createStarted;
import static org.icgc.dcc.etl2.job.export.model.ExportTables.TMP_STATIC_ROOT;

import java.util.Map;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.icgc.dcc.etl2.core.job.FileType;
import org.icgc.dcc.etl2.core.task.Task;
import org.icgc.dcc.etl2.core.task.TaskContext;
import org.icgc.dcc.etl2.core.task.TaskType;
import org.icgc.dcc.etl2.job.export.function.ExtractStats;
import org.icgc.dcc.etl2.job.export.function.ProcessDataType;
import org.icgc.dcc.etl2.job.export.function.SumDataType;
import org.icgc.dcc.etl2.job.export.model.ExportTable;
import org.icgc.dcc.etl2.job.export.model.type.DataType;
import org.icgc.dcc.etl2.job.export.util.HFileManager;
import org.icgc.dcc.etl2.job.export.util.HTableManager;

import scala.Tuple3;

import com.fasterxml.jackson.databind.node.ObjectNode;

@Slf4j
@RequiredArgsConstructor
public class ExportTableTask implements Task {

  /**
   * Configuration.
   */
  @NonNull
  private final ExportTable table;
  @NonNull
  private final Configuration conf;

  @Override
  public String getName() {
    return Task.getName(this.getClass(), table.name());
  }

  @Override
  public TaskType getType() {
    return TaskType.FILE_TYPE;
  }

  @Override
  public void execute(@NonNull TaskContext taskContext) {
    val watch = createStarted();
    val fileSystem = taskContext.getFileSystem();
    val javaSparkContext = taskContext.getSparkContext();
    val inputPath = taskContext.getPath(FileType.EXPORT_INPUT);
    log.info("Input path {} ...", inputPath);
    val dataType = createDataTypeInstance(table.name());
    log.info("Processing data type {} ...", dataType.getClass().getName());

    val dataTypeDirectoryName = dataType.getTypeDirectoryName();
    val files = getFilePaths(fileSystem, new Path(inputPath, dataTypeDirectoryName));
    val inputPaths = COMMA.join(files);
    val input = javaSparkContext.textFile(inputPaths);

    log.info("Running the base export process for data type...");
    val dataTypeProcessResult = dataType.process(input);
    log.info("Finished running the base export process.");

    log.info("Writing static export output files...");
    exportStatic(fileSystem, javaSparkContext, table.name(), dataTypeProcessResult);
    log.info("Done writing static export output files...");

    log.info("Writing dynamic export output files...");
    exportDynamic(fileSystem, dataTypeProcessResult);
    log.info("Done writing dynamic export output files...");

    log.info("Finished exporting table '{}' in {}", table, watch);
  }

  private void exportDynamic(FileSystem fileSystem, JavaRDD<ObjectNode> input) {

    log.info("Preparing data for '{}'...", table);
    val processedInput = prepareData(input);
    log.info("Finished preparing data for '{}'", table);

    log.info("Preparing export table '{}'...", table);
    val hTable = prepareHTable(conf, processedInput, table.name());
    log.info("Prepared export table: {}", table);

    log.info("Processing HFiles...");
    processHFiles(conf, fileSystem, processedInput, hTable);
    log.info("Finished processing HFiles...");
  }

  private JavaPairRDD<String, Tuple3<Map<byte[], KeyValue[]>, Long, Integer>> prepareData(JavaRDD<ObjectNode> input) {
    return input
        .zipWithIndex()
        .mapToPair(new ProcessDataType())
        .reduceByKey(new SumDataType());
  }

  private void exportStatic(FileSystem fileSystem, JavaSparkContext sparkContext, String tableName,
      JavaRDD<ObjectNode> input) {
    // TODO write static files.
    val staticOutputFile = TMP_STATIC_ROOT + tableName;
    input.coalesce(1, true).saveAsTextFile(staticOutputFile);
    walkPath(fileSystem, new Path(TMP_STATIC_ROOT));
  }

  @SneakyThrows
  private static HTable prepareHTable(Configuration conf,
      JavaPairRDD<String, Tuple3<Map<byte[], KeyValue[]>, Long, Integer>> input,
      String tableName) {
    log.info("Ensuring table...");
    val manager = new HTableManager(conf);

    if (manager.existsTable(tableName)) {
      log.info("Table exists...");
      return manager.getTable(tableName);
    }

    log.info("Calculating statistics about data...");
    val stats = input.map(new ExtractStats()).collect();
    log.info("Finished calculating statistics...");
    stats.stream().forEach(
        stat -> log.info("Donor {} has size {} and {} observation.", stat._1(), stat._2(), stat._3()));
    log.info("Preparing table...");
    val htable = manager.ensureTable(tableName, stats);

    log.info("Listing tables...");
    manager.listTables();

    return htable;
  }

  @SneakyThrows
  private static void processHFiles(Configuration conf, FileSystem fileSystem,
      JavaPairRDD<String, Tuple3<Map<byte[], KeyValue[]>, Long, Integer>> processedInput, HTable hTable) {
    val hFileManager = new HFileManager(conf, fileSystem);

    log.info("Writing HFiles...");
    hFileManager.writeHFiles(processedInput, hTable);
    log.info("Wrote HFiles");

    log.info("Loading HFiles...");
    hFileManager.loadHFiles(hTable);
    log.info("Loaded HFiles");
  }

  @SneakyThrows
  private DataType createDataTypeInstance(String tableName) {
    val packageName = "org.icgc.dcc.etl2.job.export.model.type";
    val className = packageName + "." + tableName + "DataType";
    val clazz = Class.forName(className);
    val constructor = clazz.getConstructor();
    val instance = constructor.newInstance();
    return (DataType) instance;
  }

}
