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
package org.icgc.dcc.release.job.imports.task;

import static org.icgc.dcc.release.job.imports.util.MongoJavaRDDs.javaMongoCollection;

import java.util.List;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.icgc.dcc.common.core.model.FieldNames;
import org.icgc.dcc.common.core.model.ReleaseCollection;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.task.GenericTask;
import org.icgc.dcc.release.core.task.Task;
import org.icgc.dcc.release.core.task.TaskContext;
import org.icgc.dcc.release.core.task.TaskType;
import org.icgc.dcc.release.core.util.ObjectNodes;
import org.icgc.dcc.release.job.imports.config.MongoProperties;
import org.icgc.dcc.release.job.imports.util.MongoClientURIBuilder;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.hadoop.MongoConfig;

@RequiredArgsConstructor
public class MongoImportTask extends GenericTask {

  @NonNull
  private final MongoProperties properties;
  @NonNull
  private final String database;
  @NonNull
  private final String collection;
  @NonNull
  private final FileType outputFileType;

  @Override
  public TaskType getType() {
    return TaskType.FILE_TYPE;
  }

  @Override
  public String getName() {
    return Task.getName(this.getClass(), outputFileType.toString());
  }

  @Override
  public void execute(TaskContext taskContext) {
    JavaRDD<ObjectNode> input = readInput(taskContext, createJobConf(taskContext), null);

    if (isProjectCollection(collection)) {
      input = input.filter(filterProjects(taskContext.getJobContext().getProjectNames()));
    }

    writeOutput(taskContext, input);
  }

  private static Function<ObjectNode, Boolean> filterProjects(List<String> projects) {

    return o -> {
      String projectId = ObjectNodes.textValue(o, FieldNames.PROJECT_ID);

      return projects.contains(projectId);
    };
  }

  private static boolean isProjectCollection(String collectionName) {
    return ReleaseCollection.PROJECT_COLLECTION.getId().equals(collectionName);
  }

  @Override
  protected JavaRDD<ObjectNode> readInput(TaskContext taskContext, JobConf hadoopConf, FileType inputFileType) {
    val mongoUri = new MongoClientURIBuilder()
        .uri(properties.getUri())
        .username(properties.getUserName())
        .password(properties.getPassword())
        .database(database)
        .collection(collection)
        .build();

    val mongoConfig = new MongoConfig(hadoopConf);
    mongoConfig.setInputURI(mongoUri.getURI());
    mongoConfig.setSplitSize(properties.getSplitSizeMb());

    return javaMongoCollection(taskContext.getSparkContext(), mongoConfig, hadoopConf);
  }

  protected void writeOutput(TaskContext taskContext, JavaRDD<ObjectNode> output) {
    val outputPath = new Path(taskContext.getJobContext().getWorkingDir(), outputFileType.getDirName()).toString();

    output.saveAsTextFile(outputPath);
  }

}