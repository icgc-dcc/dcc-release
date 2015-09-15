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
package org.icgc.dcc.release.job.annotate.task;

import static org.icgc.dcc.common.core.util.FormatUtils.formatBytes;
import static org.icgc.dcc.release.job.annotate.core.AnnotateJob.SSM_INPUT_TYPE;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaRDD;
import org.icgc.dcc.release.core.function.ParseObjectNode;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.task.GenericProcessTask;
import org.icgc.dcc.release.core.task.TaskContext;
import org.icgc.dcc.release.core.util.JavaRDDs;
import org.icgc.dcc.release.job.annotate.config.SnpEffProperties;
import org.icgc.dcc.release.job.annotate.function.SnpEffAnnotate;
import org.icgc.dcc.release.job.annotate.model.AnnotatedFileType;

import com.fasterxml.jackson.databind.node.ObjectNode;

@Slf4j
public class AnnotationTask extends GenericProcessTask {

  private final SnpEffProperties properties;

  public AnnotationTask(SnpEffProperties properties, FileType inputFileType, FileType outputFileType) {
    super(inputFileType, outputFileType);
    this.properties = properties;
  }

  @Override
  protected JavaRDD<ObjectNode> readInput(TaskContext taskContext) {
    return readInput(taskContext, createJobConf(taskContext));
  }

  @Override
  protected JavaRDD<ObjectNode> readInput(TaskContext taskContext, JobConf hadoopConf) {
    val maxFileSize = getMaxFileSize();

    log.info("Setting input split size of {}", formatBytes(maxFileSize));
    // TODO: Improve performance. See https://github.com/icgc-dcc/dcc-etl2/pull/6#discussion_r35150675
    val splitSize = Long.toString(maxFileSize);
    hadoopConf.set("mapred.min.split.size", splitSize);
    hadoopConf.set("mapred.max.split.size", splitSize);

    val sparkContext = taskContext.getSparkContext();
    val path = taskContext.getPath(inputFileType);
    val input = JavaRDDs.combineTextFile(sparkContext, path, hadoopConf)
        .map(tuple -> tuple._2.toString())
        .map(new ParseObjectNode());

    JavaRDDs.logPartitions(log, input.partitions());

    return input;
  }

  @Override
  protected JavaRDD<ObjectNode> process(JavaRDD<ObjectNode> input) {
    return input.mapPartitions(new SnpEffAnnotate(properties, getAnnotatedFileType()));
  }

  private AnnotatedFileType getAnnotatedFileType() {
    return inputFileType == SSM_INPUT_TYPE ? AnnotatedFileType.SSM : AnnotatedFileType.SGV;
  }

  private long getMaxFileSize() {
    return properties.getMaxFileSizeMb() * 1024L * 1024L;
  }

}