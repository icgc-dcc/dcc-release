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
package org.icgc.dcc.release.job.stage.core;

import java.util.List;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.core.model.ValueType;
import org.icgc.dcc.release.core.job.Job;
import org.icgc.dcc.release.core.job.JobContext;
import org.icgc.dcc.release.core.job.JobType;
import org.icgc.dcc.release.core.submission.SubmissionFileField;
import org.icgc.dcc.release.core.submission.SubmissionFileSchema;
import org.icgc.dcc.release.core.submission.SubmissionFileSchemas;
import org.icgc.dcc.release.core.task.Task;
import org.icgc.dcc.release.job.stage.task.DeleteStageTask;
import org.icgc.dcc.release.job.stage.task.StageFileSchemaProjectTask;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Table;

@Slf4j
@Component
@RequiredArgsConstructor(onConstructor = @__({ @Autowired }))
public class StageJob implements Job {

  /**
   * Metadata.
   */
  @NonNull
  private final SubmissionFileSchemas schemas;

  @Override
  public JobType getType() {
    return JobType.STAGE;
  }

  @Override
  public void execute(@NonNull JobContext jobContext) {
    clean(jobContext);
    stage(jobContext);
  }

  private void clean(JobContext jobContext) {
    jobContext.execute(new DeleteStageTask());
  }

  private void stage(JobContext jobContext) {
    val stagingTasks = createStagingTasks(jobContext.getFiles());
    jobContext.execute(stagingTasks);
  }

  private List<Task> createStagingTasks(Table<String, String, List<Path>> files) {
    int taskCount = 0;
    val schemaProjectTasks = ImmutableList.<Task> builder();

    for (val schemaName : files.rowKeySet()) {
      val schema = getSchema(schemaName);
      val schemaPaths = files.row(schemaName);

      for (val entry : schemaPaths.entrySet()) {
        val projectName = entry.getKey();
        val schemaProjectPaths = entry.getValue();
        val schemaProjectTask = new StageFileSchemaProjectTask(schema, projectName, schemaProjectPaths);

        log.info("[{}] Submitting task '{}'...", taskCount++, schemaProjectTask.getName());
        schemaProjectTasks.add(schemaProjectTask);
      }
    }

    return schemaProjectTasks.build();
  }


  public SubmissionFileSchema getSchema(@NonNull String schemaName) {
    SubmissionFileSchema result;

    // add PCAWG-specific column to ssm schemas
    if (schemaName.equalsIgnoreCase("ssm_m") || schemaName.equalsIgnoreCase("ssm_p")) {
      // yes, this is actually required for the fields to be included in output
      // unless we permanently add field to schema
      result = decorateSchema(schemas.get(schemaName));
    } else {
      result = schemas.get(schemaName);
    }
    return result;
  }

  private SubmissionFileSchema decorateSchema(SubmissionFileSchema schema) {
    SubmissionFileField pcawgFlag = new SubmissionFileField("pcawg_flag", ValueType.TEXT, false, null);
    // SubmissionFileField bonusFlag = new SubmissionFileField("bonus", ValueType.TEXT, false, null);
    List<SubmissionFileField> newFields = new ImmutableList.Builder<SubmissionFileField>()
        .addAll(schema.getFields())
        .add(pcawgFlag)
        // .add(bonusFlag)
        .build();
    return new SubmissionFileSchema(schema.getName(), schema.getPattern(), newFields);
  }

}
