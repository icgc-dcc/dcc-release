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
package org.icgc.dcc.release.job.export.task;

import static org.icgc.dcc.common.core.util.Separators.NEWLINE;
import static org.icgc.dcc.release.core.util.Partitions.getPartitionsCount;

import java.io.BufferedWriter;
import java.io.File;
import java.io.OutputStreamWriter;
import java.util.zip.GZIPOutputStream;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.Path;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;
import org.icgc.dcc.common.core.util.Joiners;
import org.icgc.dcc.release.core.task.GenericTask;
import org.icgc.dcc.release.core.task.TaskContext;
import org.icgc.dcc.release.job.export.function.CreateRow;
import org.icgc.dcc.release.job.export.function.KeyByDonor;
import org.icgc.dcc.release.job.export.model.ExportType;
import org.icgc.dcc.release.job.export.stats.StatsAccumulator;
import org.icgc.dcc.release.job.export.stats.StatsCalculator;
import org.icgc.dcc.release.job.export.util.StatsCalculators;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

@Slf4j
public abstract class GenericExportTask extends GenericTask {

  /**
   * Constants.
   */
  private static final String STATS_DIR_NAME = "stats";

  /**
   * Dependencies.
   */
  protected final String exportDirPath;
  protected final ExportType exportType;
  protected final StructType exportTypeSchema;
  protected final SQLContext sqlContext;

  /**
   * State.
   */
  private Accumulator<Table<String, ExportType, Long>> accumulator;

  protected GenericExportTask(@NonNull String exportDirPath, @NonNull ExportType exportType,
      @NonNull StructType exportTypeSchema, @NonNull SQLContext sqlContext) {
    this.exportDirPath = exportDirPath;
    this.exportType = exportType;
    this.exportTypeSchema = exportTypeSchema;
    this.sqlContext = sqlContext;
  }

  @Override
  public void execute(TaskContext taskContext) {
    log.info("Executing export task for '{}'...", exportType.getId());
    // ReadInput
    val input = readInput(taskContext);

    // Convert
    val dataFrame = createDataFrame(input, taskContext);

    // Save
    writeOutput(taskContext, dataFrame);

    // Save records statistics
    val donorStats = accumulator.value();
    saveStats(taskContext, donorStats);
  }

  protected abstract JavaRDD<ObjectNode> readInput(TaskContext taskContext);

  protected abstract void writeOutput(TaskContext taskContext, DataFrame output);

  protected DataFrame createDataFrame(JavaRDD<ObjectNode> input, TaskContext taskContext) {
    val partitionsNum = getPartitionsCount(input);

    accumulator = createStatsAccumulator(taskContext);
    val statsCalculator = getStatsCalculator(accumulator);
    // Convert to ROW
    val rowRdd = input.map(new CreateRow(exportType, exportTypeSchema, statsCalculator))
        .groupBy(new KeyByDonor(exportType.getIdPartitions()), partitionsNum * exportType.getParallelismMultiplier())
        .flatMap(tuple -> tuple._2);

    return sqlContext.createDataFrame(rowRdd, exportTypeSchema);
  }

  @SneakyThrows
  protected void saveStats(TaskContext taskContext, Table<String, ExportType, Long> stats) {
    @Cleanup
    val out = getStatsOutputStream(taskContext);
    for (val cell : stats.cellSet()) {
      val donorId = cell.getRowKey();
      val type = cell.getColumnKey();
      val value = cell.getValue();
      out.write(Joiners.TAB.join(donorId, type, value));
      out.write(NEWLINE);
    }
  }

  @SneakyThrows
  protected BufferedWriter getStatsOutputStream(TaskContext taskContext) {
    val statsPath = resolveStatsPath(taskContext);
    log.info("Saving donor records statistics for type {} to {}", exportType, statsPath);

    return new BufferedWriter(new OutputStreamWriter(
        new GZIPOutputStream(taskContext.getFileSystem().create(statsPath))));
  }

  protected String getOutPath(TaskContext taskContext) {
    val exportDir = resolveExportDirPath(taskContext);
    val exportTypeDir = new File(exportDir, exportType.getId());

    return exportTypeDir.getAbsolutePath();
  }

  protected File resolveExportDirPath(TaskContext taskContext) {
    val jobContext = taskContext.getJobContext();
    val workingDir = new File(jobContext.getWorkingDir());
    val exportDir = new File(workingDir, exportDirPath);

    return exportDir;
  }

  private Path resolveStatsPath(TaskContext taskContext) {
    val statsDirPath = new Path(resolveExportDirPath(taskContext).getAbsolutePath(), STATS_DIR_NAME);

    return new Path(statsDirPath, resolveStatsFileName(taskContext));
  }

  protected String resolveStatsFileName(TaskContext taskContext) {
    return exportType.getId() + ".txt.gz";
  }

  protected Accumulator<Table<String, ExportType, Long>> createStatsAccumulator(TaskContext taskContext) {
    Table<String, ExportType, Long> zeroAccumulator = HashBasedTable.create();
    val accumulator = taskContext.getSparkContext().accumulator(zeroAccumulator, new StatsAccumulator());

    return accumulator;
  }

  protected StatsCalculator getStatsCalculator(Accumulator<Table<String, ExportType, Long>> accumulator) {
    return StatsCalculators.getStatsCalculator(exportType, accumulator);
  }

}
