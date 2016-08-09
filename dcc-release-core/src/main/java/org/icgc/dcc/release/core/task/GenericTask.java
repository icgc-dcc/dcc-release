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
package org.icgc.dcc.release.core.task;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.singleton;
import static org.icgc.dcc.common.core.util.Formats.formatBytes;
import static org.icgc.dcc.common.core.util.Separators.EMPTY_STRING;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.checkExistence;
import static org.icgc.dcc.release.core.util.JavaRDDs.exists;
import static org.icgc.dcc.release.core.util.Tuples.tuple;

import java.util.List;
import java.util.regex.Pattern;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.icgc.dcc.common.hadoop.fs.HadoopUtils;
import org.icgc.dcc.release.core.document.Document;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.util.DocumentRDDs;
import org.icgc.dcc.release.core.util.HadoopFiles;
import org.icgc.dcc.release.core.util.JavaRDDs;
import org.icgc.dcc.release.core.util.ObjectNodeRDDs;
import org.icgc.dcc.release.core.util.Partitions;

import com.fasterxml.jackson.databind.node.ObjectNode;

@Slf4j
public abstract class GenericTask implements Task {

  private static final Pattern PARTITION_NAME_PATTERN = Pattern.compile(Partitions.PARTITION_NAME + ".*");

  private final String name;

  public GenericTask(String name) {
    this.name = Task.getName(this.getClass(), name);
  }

  public GenericTask() {
    this.name = Task.getName(this.getClass());
  }

  @Override
  public String getName() {
    return name;
  }

  protected JobConf createJobConf(TaskContext taskContext) {
    val sparkContext = taskContext.getSparkContext();

    return new JobConf(sparkContext.hadoopConfiguration());
  }

  protected JavaRDD<ObjectNode> readInput(TaskContext taskContext, FileType inputFileType) {
    val conf = createJobConf(taskContext);

    return readInput(taskContext, conf, inputFileType);
  }

  /**
   * @param size split/combine size in MBytes
   */
  protected JavaRDD<ObjectNode> readInput(TaskContext taskContext, FileType inputFileType, int size) {
    val conf = createJobConf(taskContext);

    return readInput(taskContext, conf, inputFileType, size);
  }

  /**
   * @param size split/combine size in MBytes
   */
  protected JavaRDD<ObjectNode> readInput(TaskContext taskContext, JobConf hadoopConf, FileType inputFileType, int size) {
    val maxFileSize = size * 1024L * 1024L;

    log.debug("Setting input split size of {}", formatBytes(maxFileSize));
    val splitSize = Long.toString(maxFileSize);
    hadoopConf.set("mapred.min.split.size", splitSize);
    hadoopConf.set("mapred.max.split.size", splitSize);

    val sparkContext = taskContext.getSparkContext();
    val path = taskContext.getPath(inputFileType);
    if (!checkExistence(taskContext.getFileSystem(), path)) {
      return taskContext.getSparkContext().emptyRDD();
    }

    val input = taskContext.isCompressOutput() ?
        ObjectNodeRDDs.combineObjectNodeSequenceFile(sparkContext, path, hadoopConf) :
        ObjectNodeRDDs.combineObjectNodeFile(sparkContext, path, hadoopConf);

    JavaRDDs.logPartitions(log, input.partitions());

    return input;
  }

  protected JavaRDD<ObjectNode> readInput(TaskContext taskContext, JobConf conf, FileType inputFileType) {
    return readInput(taskContext, conf, inputFileType, ObjectNode.class);
  }

  protected <T> JavaRDD<T> readInput(TaskContext taskContext, JobConf conf, FileType inputFileType, Class<T> clazz) {
    if (isReadAll(taskContext, inputFileType)) {
      return readAllInput(taskContext, conf, inputFileType, clazz);
    }

    val sparkContext = taskContext.getSparkContext();
    val filePath = taskContext.getPath(inputFileType);

    if (!exists(sparkContext, filePath)) {
      log.debug("{} does not exist. Skipping...", filePath);

      return sparkContext.emptyRDD();
    }

    val input = readInput(taskContext, taskContext.getPath(inputFileType), conf, clazz);

    return input;
  }

  protected JavaRDD<ObjectNode> readUnpartitionedInput(TaskContext taskContext, FileType inputFileType) {
    val filePath = taskContext.getPath(inputFileType);
    val sparkContext = taskContext.getSparkContext();
    if (!exists(sparkContext, filePath)) {
      log.debug("{} does not exist. Skipping...", filePath);

      return sparkContext.emptyRDD();
    }

    val conf = createJobConf(taskContext);

    return readAllInput(taskContext, conf, inputFileType, ObjectNode.class);
  }

  protected JavaRDD<ObjectNode> readDocInput(TaskContext taskContext, FileType inputFileType) {
    if (taskContext.isCompressOutput()) {
      return readSequenceFileInput(taskContext, inputFileType);
    } else {
      return readInput(taskContext, inputFileType);
    }
  }

  protected JavaPairRDD<String, ObjectNode> readUnpartitionedSequenceFileInput(TaskContext taskContext,
      FileType inputFileType) {
    val filePath = taskContext.getPath(inputFileType);
    val sparkContext = taskContext.getSparkContext();
    if (!exists(sparkContext, filePath)) {
      log.debug("{} does not exist. Skipping...", filePath);
      JavaRDD<ObjectNode> emptyRDD = sparkContext.emptyRDD();

      return emptyRDD.mapToPair(objectNode -> tuple(EMPTY_STRING, objectNode));
    }

    val conf = createJobConf(taskContext);

    return readAllSequenceFileInput(taskContext, conf, inputFileType, ObjectNode.class);
  }

  protected void writeOutput(TaskContext taskContext, JavaRDD<ObjectNode> processed, FileType outputFileType) {
    val outputPath = taskContext.getPath(outputFileType);

    writeOutput(processed, outputPath, taskContext.isCompressOutput());
  }

  protected <T> void writeOutput(TaskContext taskContext, JavaRDD<T> processed, FileType outputFileType, Class<T> clazz) {
    val outputPath = taskContext.getPath(outputFileType);

    writeOutput(processed, outputPath, taskContext.isCompressOutput(), clazz);
  }

  protected void writeOutput(JavaRDD<ObjectNode> processed, String outputPath, boolean compressOutput) {
    if (compressOutput) {
      ObjectNodeRDDs.saveAsSequenceObjectNodeFile(processed, outputPath);
    } else {
      ObjectNodeRDDs.saveAsTextObjectNodeFile(processed, outputPath);
    }
  }

  protected <T> void writeOutput(JavaRDD<T> processed, String outputPath, boolean compressOutput,
      Class<T> clazz) {
    if (compressOutput) {
      ObjectNodeRDDs.saveAsSequenceObjectNodeFile(processed, outputPath, clazz);
    } else {
      ObjectNodeRDDs.saveAsTextObjectNodeFile(processed, outputPath, clazz);
    }
  }

  protected void writeDocOutput(TaskContext taskContext, JavaRDD<Document> processed, String outputPath) {
    if (taskContext.isCompressOutput()) {
      DocumentRDDs.saveAsSequenceIdObjectNodeFile(processed, outputPath);
    } else {
      DocumentRDDs.saveAsTextObjectNodeFile(processed, outputPath);
    }
  }

  private JavaRDD<ObjectNode> readSequenceFileInput(TaskContext taskContext, FileType inputFileType) {
    val conf = createJobConf(taskContext);
    if (isReadAll(taskContext, inputFileType)) {
      return readAllSequenceFileInput(taskContext, conf, inputFileType, ObjectNode.class).values();
    }

    val sparkContext = taskContext.getSparkContext();
    val filePath = taskContext.getPath(inputFileType);
    if (!exists(sparkContext, filePath)) {
      log.debug("{} does not exist. Skipping...", filePath);

      return sparkContext.emptyRDD();
    }

    return readSequenceFileInput(taskContext, taskContext.getPath(inputFileType), conf, ObjectNode.class).values();
  }

  private static <T> JavaRDD<T> readAllInput(TaskContext taskContext, JobConf conf, FileType inputFileType,
      Class<T> clazz) {
    val fileTypePath = new Path(taskContext.getJobContext().getWorkingDir(), inputFileType.getDirName());
    val inputPaths = resolveInputPaths(taskContext, fileTypePath);

    return inputPaths.stream()
        .peek(inputPath -> log.debug("Reading {} ...", inputPath)) // Optional
        .map(inputPath -> readInput(taskContext, inputPath.toString(), conf, clazz))
        .reduce((x, y) -> x.union(y)).get();
  }

  private static <T> JavaRDD<T> readInput(TaskContext taskContext, String path, JobConf conf, Class<T> clazz) {
    val sparkContext = taskContext.getSparkContext();
    if (taskContext.isCompressOutput()) {
      return HadoopFiles.sequenceFile(sparkContext, path, conf, clazz);
    } else {
      return HadoopFiles.textFile(sparkContext, path, conf, clazz);
    }
  }

  private static <T> JavaPairRDD<String, T> readAllSequenceFileInput(TaskContext taskContext, JobConf conf,
      FileType inputFileType, Class<T> clazz) {
    val fileTypePath = new Path(taskContext.getJobContext().getWorkingDir(), inputFileType.getDirName());
    val inputPaths = inputFileType.isPartitioned() ?
        resolveInputPaths(taskContext, fileTypePath) :
        singleton(fileTypePath);

    return inputPaths.stream()
        .peek(inputPath -> log.debug("Reading {} ...", inputPath)) // Optional
        .map(inputPath -> readSequenceFileInput(taskContext, inputPath.toString(), conf, clazz))
        .reduce((x, y) -> x.union(y)).get();
  }

  private static <T> JavaPairRDD<String, T> readSequenceFileInput(TaskContext taskContext, String path, JobConf conf,
      Class<T> clazz) {
    val sparkContext = taskContext.getSparkContext();
    checkArgument(taskContext.isCompressOutput(), "Method doesn't support reading uncompressed input.");

    return HadoopFiles.sequenceFileWithKey(sparkContext, path, conf, clazz);
  }

  private static List<Path> resolveInputPaths(TaskContext taskContext, Path fileTypePath) {
    return HadoopUtils.lsDir(taskContext.getFileSystem(), fileTypePath, PARTITION_NAME_PATTERN);
  }

  private static boolean isReadAll(TaskContext taskContext, FileType inputFileType) {
    return inputFileType.isPartitioned() && !taskContext.getProjectName().isPresent();
  }

}