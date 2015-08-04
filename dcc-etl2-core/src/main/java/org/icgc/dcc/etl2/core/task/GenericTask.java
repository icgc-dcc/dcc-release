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
package org.icgc.dcc.etl2.core.task;

import static org.icgc.dcc.etl2.core.util.JavaRDDs.emptyRDD;
import static org.icgc.dcc.etl2.core.util.JavaRDDs.exists;
import static org.icgc.dcc.etl2.core.util.JavaRDDs.logPartitions;
import static org.icgc.dcc.etl2.core.util.ObjectNodeRDDs.combineObjectNodeFile;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaRDD;
import org.icgc.dcc.etl2.core.job.FileType;
import org.icgc.dcc.etl2.core.util.ObjectNodeRDDs;

import com.fasterxml.jackson.databind.node.ObjectNode;

@Slf4j
public abstract class GenericTask implements Task {

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

  protected JavaRDD<ObjectNode> readInput(TaskContext taskContext, FileType inputFileType, String path) {
    val conf = createJobConf(taskContext);

    return readInput(taskContext, conf, inputFileType, path);
  }

  protected JavaRDD<ObjectNode> readInput(TaskContext taskContext, JobConf conf, FileType inputFileType) {
    return readInput(taskContext, conf, inputFileType, "");
  }

  protected JavaRDD<ObjectNode> readInput(TaskContext taskContext, JobConf conf, FileType inputFileType, String path) {
    val sparkContext = taskContext.getSparkContext();
    val filePath = taskContext.getPath(inputFileType) + path;

    if (!exists(sparkContext, filePath)) {
      log.warn("{} does not exist. Skipping...", filePath);

      return emptyRDD(sparkContext);
    }

    val splitSize = Long.toString(48 * 1024 * 1024);
    conf.set("mapred.min.split.size", splitSize);
    conf.set("mapred.max.split.size", splitSize);

    val input = combineObjectNodeFile(sparkContext, taskContext.getPath(inputFileType) + path, conf);
    logPartitions(log, input.partitions());

    return input;
    // return ObjectNodeRDDs.textObjectNodeFile(sparkContext, taskContext.getPath(inputFileType) + path, conf);
    // return ObjectNodeRDDs.sequenceObjectNodeFile(sparkContext, taskContext.getPath(inputFileType), conf);
  }

  protected void writeOutput(TaskContext taskContext, JavaRDD<ObjectNode> processed, FileType outputFileType) {
    val outputPath = taskContext.getPath(outputFileType);

    writeOutput(processed, outputPath);
  }

  protected void writeOutput(JavaRDD<ObjectNode> processed, String outputPath) {
    ObjectNodeRDDs.saveAsTextObjectNodeFile(processed, outputPath);
    // ObjectNodeRDDs.saveAsSequenceObjectNodeFile(processed, outputPath);
  }

}