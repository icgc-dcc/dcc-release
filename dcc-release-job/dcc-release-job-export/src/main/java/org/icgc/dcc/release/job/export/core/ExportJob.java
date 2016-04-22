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

import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableMap;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.checkExistence;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.mkdirs;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.rmr;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;
import org.icgc.dcc.release.core.job.GenericJob;
import org.icgc.dcc.release.core.job.JobContext;
import org.icgc.dcc.release.core.job.JobType;
import org.icgc.dcc.release.core.task.Task;
import org.icgc.dcc.release.job.export.config.ExportProperties;
import org.icgc.dcc.release.job.export.model.ExportType;
import org.icgc.dcc.release.job.export.task.ExportProjectTask;
import org.icgc.dcc.release.job.export.task.ExportTask;
import org.icgc.dcc.release.job.export.util.SchemaGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class ExportJob extends GenericJob {

  /**
   * Dependencies.
   */
  @NonNull
  private final ExportProperties exportProperties;
  @NonNull
  private final FileSystem fileSystem;
  @NonNull
  private final JavaSparkContext sparkContext;

  @Override
  public JobType getType() {
    return JobType.EXPORT;
  }

  @Override
  public void execute(@NonNull JobContext jobContext) {
    clean(jobContext);
    export(jobContext);
  }

  private void clean(JobContext jobContext) {
    val outputDir = getOutputDir(jobContext);

    if (checkExistence(fileSystem, outputDir)) {
      log.info("Deleting directory {} ...", outputDir);
      rmr(fileSystem, outputDir);
    }

    log.info("Creating directory {} ...", outputDir);
    mkdirs(fileSystem, outputDir);
  }

  private void export(JobContext jobContext) {
    val generator = new SchemaGenerator();
    val dataTypes = initDataTypes(generator);

    for (val task : createTasks(dataTypes)) {
      if (task instanceof ExportProjectTask) {
        jobContext.executeSequentially(task);
      } else {
        jobContext.execute(task);
      }
    }
  }

  private List<Task> createTasks(Map<ExportType, StructType> dataTypes) {
    val sqlContext = createSqlContext();
    val runExportTypes = exportProperties.getExportTypes();

    return dataTypes.entrySet().stream()
        .filter(dt -> {
          if (runExportTypes.isEmpty()) {
            return true;
          } else {
            return runExportTypes.contains(dt.getKey().getId());
          }
        })
        .map(createTask(sqlContext))
        .collect(toImmutableList());
  }

  private Function<Entry<ExportType, StructType>, ? extends Task> createTask(SQLContext sqlContext) {
    return e -> {
      ExportType type = e.getKey();
      if (type.isSplitByProject()) {
        return new ExportProjectTask(exportProperties.getExportDir(), type, e.getValue(), sqlContext);
      } else {
        return new ExportTask(exportProperties.getExportDir(), type, e.getValue(), sqlContext);
      }
    };
  }

  private Path getOutputDir(JobContext jobContext) {
    return new Path(new Path(jobContext.getWorkingDir()), exportProperties.getExportDir());
  }

  private static Map<ExportType, StructType> initDataTypes(SchemaGenerator generator) {
    return ExportType.getExportTypes().stream()
        .collect(toImmutableMap(et -> et, et -> generator.createDataType(et)));
  }

  private SQLContext createSqlContext() {
    val sqlContext = new SQLContext(sparkContext);
    sqlContext.setConf("spark.sql.parquet.compression.codec", exportProperties.getCompressionCodec());

    return sqlContext;
  }

}
