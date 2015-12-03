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
package org.icgc.dcc.release.job.stage.task;

import static org.icgc.dcc.common.core.util.Joiners.COMMA;

import java.util.List;

import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;
import nl.basjes.hadoop.io.compress.SplittableGzipCodec;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.icgc.dcc.release.core.function.TranslateMissingCode;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.submission.SubmissionFileSchema;
import org.icgc.dcc.release.core.task.GenericTask;
import org.icgc.dcc.release.core.task.TaskContext;
import org.icgc.dcc.release.core.task.TaskType;
import org.icgc.dcc.release.core.util.Configurations;
import org.icgc.dcc.release.core.util.JavaRDDs;
import org.icgc.dcc.release.core.util.Partitions;
import org.icgc.dcc.release.job.stage.function.CleanSensitiveFields;
import org.icgc.dcc.release.job.stage.function.ConvertValueType;
import org.icgc.dcc.release.job.stage.function.ParseFileSplit;
import org.icgc.dcc.release.job.stage.function.TranslateCodeListTerm;
import org.icgc.dcc.release.job.stage.function.TrimValues;

import com.fasterxml.jackson.databind.node.ObjectNode;

@Slf4j
public class StageFileSchemaProjectTask extends GenericTask {

  /**
   * Configuration.
   */
  private final SubmissionFileSchema schema;
  private final String projectName;
  private transient final List<Path> schemaProjectPaths;

  public StageFileSchemaProjectTask(SubmissionFileSchema schema, String projectName, List<Path> schemaProjectPaths) {
    super(schema.getName() + ":" + projectName);
    this.schema = schema;
    this.projectName = projectName;
    this.schemaProjectPaths = schemaProjectPaths;
  }

  @Override
  public TaskType getType() {
    return TaskType.FILE_TYPE;
  }

  @Override
  public void execute(TaskContext taskContext) {
    val input = readInput(taskContext);
    val processed = transform(input);

    writeOutput(processed, getOutputPath(taskContext), taskContext.isCompressOutput());
  }

  private JavaRDD<ObjectNode> readInput(TaskContext taskContext) {
    val sparkContext = taskContext.getSparkContext();
    val paths = formatInputPaths(schemaProjectPaths);

    val conf = createJobConf(taskContext);

    val minLength = getMinInputFileLength(taskContext.getFileSystem());
    val large = minLength > 128L * 1024L * 1024L;
    if (large) {
      // Add splittable gzip codec
      Configurations.addCompressionCodec(conf, SplittableGzipCodec.class);
    }

    val input = JavaRDDs.textFile(sparkContext, paths, conf);

    log.info("Input paths: {}", paths);
    JavaRDDs.logPartitions(log, input.partitions());

    return input.mapPartitionsWithInputSplit(new ParseFileSplit(schema), false);
  }

  @SneakyThrows
  private long getMinInputFileLength(FileSystem fileSystem) {
    long minLength = Long.MAX_VALUE;
    for (val path : schemaProjectPaths) {
      val length = fileSystem.getFileStatus(path).getLen();
      if (length < minLength) {
        minLength = length;
      }
    }

    return minLength;
  }

  private JavaRDD<ObjectNode> transform(JavaRDD<ObjectNode> input) {
    return input
        .map(new TrimValues())
        .map(new TranslateMissingCode())
        .map(new TranslateCodeListTerm(schema))
        .map(new ConvertValueType(schema))
        .map(new CleanSensitiveFields(schema));
  }

  private String getOutputPath(TaskContext taskContext) {
    val outputFileType = getOutputFileType();
    val outputDir = new Path(taskContext.getJobContext().getWorkingDir(), outputFileType.getDirName());

    return new Path(outputDir, Partitions.getPartitionName(projectName)).toString();
  }

  private FileType getOutputFileType() {
    return FileType.valueOf(schema.getName().toUpperCase());
  }

  private static String formatInputPaths(Iterable<?> paths) {
    return COMMA.join(paths);
  }

}