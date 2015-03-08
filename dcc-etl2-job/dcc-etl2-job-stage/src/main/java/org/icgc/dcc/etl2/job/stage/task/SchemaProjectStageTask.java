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
package org.icgc.dcc.etl2.job.stage.task;

import static org.icgc.dcc.common.core.util.Joiners.COMMA;

import java.util.List;

import lombok.val;

import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.icgc.dcc.etl2.core.function.TranslateMissingCode;
import org.icgc.dcc.etl2.core.job.FileType;
import org.icgc.dcc.etl2.core.submission.Schema;
import org.icgc.dcc.etl2.core.task.GenericTask;
import org.icgc.dcc.etl2.core.task.TaskContext;
import org.icgc.dcc.etl2.core.task.TaskType;
import org.icgc.dcc.etl2.core.util.JavaRDDs;
import org.icgc.dcc.etl2.core.util.Partitions;
import org.icgc.dcc.etl2.job.stage.function.ConvertValueType;
import org.icgc.dcc.etl2.job.stage.function.ParseLine;
import org.icgc.dcc.etl2.job.stage.function.TranslateCodeListTerm;
import org.icgc.dcc.etl2.job.stage.function.TrimValues;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class SchemaProjectStageTask extends GenericTask {

  /**
   * Configuration.
   */
  private final Schema schema;
  private final String projectName;
  private final List<Path> schemaProjectPaths;

  public SchemaProjectStageTask(Schema schema, String projectName, List<Path> schemaProjectPaths) {
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
    val sparkContext = taskContext.getSparkContext();

    val input = readInput(sparkContext);
    val processed = transform(input);

    writeOutput(processed, getOutputPath(taskContext));
  }

  private JavaRDD<ObjectNode> readInput(JavaSparkContext sparkContext) {
    val projectPaths = formatProjectInputPaths();

    return JavaRDDs.javaHadoopRDD(sparkContext, projectPaths)
        .mapPartitionsWithInputSplit(new ParseLine(schema), false);
  }

  private JavaRDD<ObjectNode> transform(JavaRDD<ObjectNode> input) {
    return input
        .map(new TrimValues())
        .map(new TranslateMissingCode())
        .map(new TranslateCodeListTerm(schema))
        .map(new ConvertValueType(schema));
  }

  private String formatProjectInputPaths() {
    return COMMA.join(schemaProjectPaths);
  }

  private String getOutputPath(TaskContext taskContext) {
    val outputFileType = getOutputFileType();
    val outputDir = new Path(taskContext.getJobContext().getWorkingDir(), outputFileType.getDirName());

    return new Path(outputDir, Partitions.getPartitionName(projectName)).toString();
  }

  public FileType getOutputFileType() {
    return FileType.valueOf(schema.getName().toUpperCase());
  }

}