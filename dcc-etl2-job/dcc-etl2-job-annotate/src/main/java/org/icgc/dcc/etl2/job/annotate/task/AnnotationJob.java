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
package org.icgc.dcc.etl2.job.annotate.task;

import static org.icgc.dcc.common.core.util.FormatUtils.formatBytes;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaRDD;
import org.icgc.dcc.etl2.core.function.ParseObjectNode;
import org.icgc.dcc.etl2.core.function.TranslateMissingCode;
import org.icgc.dcc.etl2.core.job.FileType;
import org.icgc.dcc.etl2.core.task.GenericProcessTask;
import org.icgc.dcc.etl2.core.task.TaskContext;
import org.icgc.dcc.etl2.core.util.JavaRDDs;
import org.icgc.dcc.etl2.job.annotate.config.SnpEffProperties;
import org.icgc.dcc.etl2.job.annotate.function.SnpEffAnnotate;
import org.icgc.dcc.etl2.job.annotate.model.AnnotatedFileType;

import com.fasterxml.jackson.databind.node.ObjectNode;

@Slf4j
public class AnnotationJob extends GenericProcessTask {

  private final SnpEffProperties properties;

  public AnnotationJob(SnpEffProperties properties, FileType inputFileType, FileType outputFileType) {
    super(inputFileType, outputFileType);
    this.properties = properties;
  }

  @Override
  protected JavaRDD<ObjectNode> readInput(TaskContext taskContext, JobConf hadoopConf) {
    val maxFileSize = getMaxFileSize();

    log.info("Setting input split size of {}", formatBytes(maxFileSize));
    val splitSize = Long.toString(maxFileSize);
    hadoopConf.set("mapred.min.split.size", splitSize);
    hadoopConf.set("mapred.max.split.size", splitSize);

    val sparkContext = taskContext.getSparkContext();
    val path = taskContext.getPath(inputFileType);
    val input = JavaRDDs.javaCombineTextFile(sparkContext, path, hadoopConf).map(new ParseObjectNode());

    logPartitions(input);

    return input;
  }

  @Override
  protected JavaRDD<ObjectNode> process(JavaRDD<ObjectNode> input) {
    return input
        .mapPartitions(
            new SnpEffAnnotate(properties, getAnnotatedFileType()))
        // TODO: Design this out of this module as this is a submission concept
        .map(new TranslateMissingCode());
  }

  private AnnotatedFileType getAnnotatedFileType() {
    return inputFileType == FileType.SSM_P ? AnnotatedFileType.SSM : AnnotatedFileType.SGV;
  }

  private long getMaxFileSize() {
    return properties.getMaxFileSizeMb() * 1024L * 1024L;
  }

}