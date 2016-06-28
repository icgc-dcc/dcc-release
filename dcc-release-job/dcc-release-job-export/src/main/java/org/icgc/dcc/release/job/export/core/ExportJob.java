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

import static java.util.Collections.singleton;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableMap;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.checkExistence;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.mkdirs;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.rmr;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;
import org.icgc.dcc.common.core.model.DownloadDataType;
import org.icgc.dcc.common.core.model.Marking;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.job.GenericJob;
import org.icgc.dcc.release.core.job.JobContext;
import org.icgc.dcc.release.core.job.JobType;
import org.icgc.dcc.release.core.task.Task;
import org.icgc.dcc.release.job.export.config.ExportProperties;
import org.icgc.dcc.release.job.export.function.gzip.ClinicalRecordConverter;
import org.icgc.dcc.release.job.export.function.gzip.DefaultRecordConverter;
import org.icgc.dcc.release.job.export.function.gzip.RecordConverter;
import org.icgc.dcc.release.job.export.function.gzip.SecondaryRecordConverter;
import org.icgc.dcc.release.job.export.function.gzip.SsmRecordConverter;
import org.icgc.dcc.release.job.export.io.GzipRowWriter;
import org.icgc.dcc.release.job.export.io.RowWriter;
import org.icgc.dcc.release.job.export.model.ExportType;
import org.icgc.dcc.release.job.export.task.ExportProjectTask;
import org.icgc.dcc.release.job.export.task.ExportTask;
import org.icgc.dcc.release.job.export.task.WriteHeadersTask;
import org.icgc.dcc.release.job.export.util.SchemaGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableSet;

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
    if (exportProperties.isClean()) {
      clean(jobContext);
    }

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
    val tasks = createTasks();
    if (exportProperties.isSequential()) {
      executeTasksSequentially(jobContext, tasks);
    } else {
      executeTasksParallel(jobContext, tasks);
    }

    // Write header files
    val task = new WriteHeadersTask(exportProperties.getExportDir());
    jobContext.execute(task);
  }

  private Iterable<Task> createTasks() {
    val runExportTypes = exportProperties.getExportTypes();

    return stream(DownloadDataType.values())
        .filter(filterExportDataTypes(runExportTypes))
        .map(createTask())
        .collect(toImmutableSet());
  }

  private Function<DownloadDataType, ? extends Task> createTask() {
    val sqlContext = exportProperties.isExportParquet() ?
        Optional.of(createSqlContext()) :
        Optional.<SQLContext> empty();

    return dataType -> {
      return sqlContext.isPresent() ?
          createTaskWithParquet(dataType, sqlContext.get()) :
          createGzipTask(dataType);
    };
  }

  private Task createGzipTask(DownloadDataType dataType) {
    log.debug("Creating task for data type: {}", dataType);

    return new ExportTask(resolveFileType(dataType), createOutputWriter(dataType));
  }

  private Iterable<RowWriter> createOutputWriter(DownloadDataType dataType) {
    return singleton(new GzipRowWriter(
        exportProperties.getExportDir(),
        resolveRecordConverter(dataType, exportProperties.getMaxPartitions())));
  }

  private Path getOutputDir(JobContext jobContext) {
    return new Path(new Path(jobContext.getWorkingDir()), exportProperties.getExportDir());
  }

  private SQLContext createSqlContext() {
    val sqlContext = new SQLContext(sparkContext);
    sqlContext.setConf("spark.sql.parquet.compression.codec", exportProperties.getSqlCompressionCodec());

    return sqlContext;
  }

  private static RecordConverter resolveRecordConverter(DownloadDataType dataType, int maxPartitions) {
    if (dataType == DownloadDataType.DONOR) {
      return new ClinicalRecordConverter();
    }

    if (dataType == DownloadDataType.SSM_OPEN) {
      return new SsmRecordConverter(dataType, maxPartitions, ImmutableSet.of(Marking.OPEN, Marking.MASKED));
    }

    if (dataType == DownloadDataType.SSM_CONTROLLED) {
      return new SsmRecordConverter(dataType, maxPartitions, ImmutableSet.of(Marking.OPEN, Marking.CONTROLLED));
    }

    if (dataType.isSecondaryDataType()) {
      return new SecondaryRecordConverter(dataType, maxPartitions);
    }

    return new DefaultRecordConverter(dataType, maxPartitions);
  }

  private static FileType resolveFileType(DownloadDataType dataType) {
    switch (dataType) {
    case DONOR:
      return FileType.CLINICAL;
    default:
      return FileType.getFileType(dataType.getCanonicalName());
    }
  }

  private static void executeTasksParallel(JobContext jobContext, Iterable<Task> tasks) {
    for (val task : tasks) {
      if (task instanceof ExportProjectTask) {
        jobContext.executeSequentially(task);
      } else {
        jobContext.execute(task);
      }
    }
  }

  private static Task createTaskWithParquet(DownloadDataType dataType, SQLContext sqlContext) {
    val generator = new SchemaGenerator();
    @SuppressWarnings("unused")
    val dataTypes = initDataTypes(generator);
    // The tasks just need to be properly created.

    throw new UnsupportedOperationException();
  }

  private static void executeTasksSequentially(JobContext jobContext, Iterable<Task> tasks) {
    for (val task : tasks) {
      jobContext.executeSequentially(task);
    }
  }

  private static Map<ExportType, StructType> initDataTypes(SchemaGenerator generator) {
    return ExportType.getExportTypes().stream()
        .collect(toImmutableMap(et -> et, et -> generator.createDataType(et)));
  }

  private static Predicate<DownloadDataType> filterExportDataTypes(List<String> runExportTypes) {
    return dt -> {
      // Don't create tasks for DONOR_EXPOSURE etc. as all the DONOR sub-types will be exported in the export DONOR job
      if (dt.isClinicalSubtype()) {
        return false;
      }

      if (runExportTypes.isEmpty()) {
        return true;
      }

      return runExportTypes.contains(dt.getCanonicalName());
    };
  }

}
