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

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaRDD;
import org.icgc.dcc.etl2.core.job.FileType;

import com.fasterxml.jackson.databind.node.ObjectNode;

@Slf4j
public abstract class GenericProcessTask extends GenericTask {

  /**
   * Configuration.
   */
  protected final FileType inputFileType;
  protected final FileType outputFileType;

  public GenericProcessTask(FileType inputFileType, FileType outputFileType) {
    super(outputFileType.getDirName());
    this.inputFileType = inputFileType;
    this.outputFileType = outputFileType;
  }

  @Override
  public void execute(TaskContext taskContext) {
    if (!hasInput(taskContext)) {
      log.info("[{}] No input for '{}' and output '{}'. Skipping...", getName(), inputFileType, outputFileType);
      return;
    }

    log.info("[{}] Initializing...", getName());
    init(taskContext);

    val input = readInput(taskContext);

    val processed = process(input);

    writeOutput(taskContext, processed);
  }

  /**
   * Template method.
   */
  protected abstract JavaRDD<ObjectNode> process(JavaRDD<ObjectNode> input);

  protected boolean hasInput(TaskContext taskContext) {
    return taskContext.exists(inputFileType);
  }

  protected void init(TaskContext taskContext) {
    taskContext.delete(outputFileType);
  }

  protected JavaRDD<ObjectNode> readInput(TaskContext taskContext) {
    return super.readInput(taskContext, inputFileType);
  }

  protected JavaRDD<ObjectNode> readInput(TaskContext taskContext, JobConf hadoopConf) {
    return super.readInput(taskContext, hadoopConf, inputFileType);
  }

  protected void writeOutput(TaskContext taskContext, JavaRDD<ObjectNode> processed) {
    super.writeOutput(taskContext, processed, outputFileType);
  }

}