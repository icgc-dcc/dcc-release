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

import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.etl2.core.util.Stopwatches.createStarted;

import java.util.UUID;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.icgc.dcc.etl2.core.job.FileType;
import org.icgc.dcc.etl2.core.task.Task;
import org.icgc.dcc.etl2.core.task.TaskContext;
import org.icgc.dcc.etl2.core.task.TaskType;
import org.icgc.dcc.etl2.job.export.model.ExportTable;
import org.icgc.dcc.etl2.job.export.model.ExportTables;
import org.icgc.dcc.etl2.job.export.util.HFileLoadJobFactory;
import org.icgc.dcc.etl2.job.export.util.HFileLoader;
import org.icgc.dcc.etl2.job.export.util.HFileWriter;
import org.icgc.dcc.etl2.job.export.util.HTableManager;
import org.icgc.dcc.etl2.job.export.util.InputKeyResolver;
import org.icgc.dcc.etl2.job.export.util.SplitKeyCalculator;

@Slf4j
@RequiredArgsConstructor
public class ExportTableTask implements Task {

  /**
   * Constants.
   */
  private static final Path HFILE_DIR_PATH = new Path("/tmp");

  /**
   * Configuration.
   */
  @NonNull
  private final ExportTable table;
  @NonNull
  private final Configuration conf;
  // TODO: Correct this value and externalize
  private final long regionSize = ExportTables.MAX_DATA_FILE_SIZE;

  @Override
  public String getName() {
    return "export:" + table.name();
  }

  @Override
  public TaskType getType() {
    return TaskType.FILE_TYPE;
  }

  @Override
  public void execute(@NonNull TaskContext taskContext) {
    val watch = createStarted();
    val fileSystem = taskContext.getFileSystem();
    val sparkContext = taskContext.getSparkContext();

    log.info("Getting input path...");
    val inputPath = getInputPath(taskContext);
    log.info("Got input path '{}'", inputPath);

    log.info("Resolving input keys...");
    val keys = resolveInputKeys(sparkContext, inputPath);
    log.info("Resolved input keys");

    log.info("Calculating input path split keys...");
    val splitKeys = calculateSplitKeys(regionSize, inputPath, keys);
    log.info("Calculated {} input path split keys: {}", formatCount(splitKeys), splitKeys);

    log.info("Preparing export table '{}'...", table);
    val hTable = prepareHTable(conf, splitKeys);
    log.info("Prepared export table: {}", table);

    log.info("Creating load job...");
    val hFileLoadJob = createHFileLoadJob(conf, hTable);
    log.info("Created load job: {}", hFileLoadJob);

    log.info("Getting HFile path...");
    val hFilePath = getHFilePath(fileSystem, hTable);
    log.info("Got HFile path: '{}'", hFilePath);

    log.info("Writting HFiles...");
    writeHFiles(sparkContext, hFileLoadJob.getConfiguration(), inputPath, hTable, hFilePath);
    log.info("Wrote HFiles");

    log.info("Loading HFiles...");
    loadHFiles(conf, fileSystem, hTable, hFilePath);
    log.info("Loaded HFiles");

    log.info("Finished exporting table '{}' in {}", table, watch);
  }

  private static Path getInputPath(TaskContext taskContext) {
    return new Path(taskContext.getPath(FileType.EXPORT_INPUT));
  }

  private JavaRDD<String> resolveInputKeys(JavaSparkContext sparkContext, Path inputPath) {
    val resolver = new InputKeyResolver(sparkContext);

    return resolver.resolveKeys(inputPath);
  }

  private Iterable<String> calculateSplitKeys(long regionSize, Path inputPath, JavaRDD<String> keys) {
    val calculator = new SplitKeyCalculator(conf);

    return calculator.calculateSplitKeys(inputPath, keys, regionSize);
  }

  @SneakyThrows
  private HTable prepareHTable(Configuration conf, Iterable<String> splitKeys) {
    @Cleanup
    val admin = new HBaseAdmin(conf);
    val manager = new HTableManager(admin);

    log.info("Ensuring table...");
    val htable = manager.ensureTable(table.name(), splitKeys);

    log.info("Listing tables...");
    manager.listTables();

    return htable;
  }

  private Job createHFileLoadJob(Configuration conf, HTable table) {
    val factory = new HFileLoadJobFactory(conf);

    return factory.createJob(table);
  }

  private static void writeHFiles(JavaSparkContext sparkContext, Configuration conf, Path inputPath, HTable table,
      Path hFilePath) {
    val writer = new HFileWriter(conf, table, sparkContext);

    writer.writeHFiles(inputPath, hFilePath);
  }

  private static void loadHFiles(Configuration conf, FileSystem fileSystem, HTable hTable, Path hFilePath) {
    val loader = new HFileLoader(conf, fileSystem);

    loader.loadHFiles(hTable, hFilePath);
  }

  private static Path getHFilePath(FileSystem fileSystem, HTable hTable) {
    val hFileName = new String(hTable.getTableName()) + "_" + UUID.randomUUID();
    val hFilePath = new Path(HFILE_DIR_PATH, hFileName);

    return fileSystem.makeQualified(hFilePath);
  }

}
