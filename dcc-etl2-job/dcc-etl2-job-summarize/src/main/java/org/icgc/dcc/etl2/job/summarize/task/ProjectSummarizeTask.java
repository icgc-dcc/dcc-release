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
package org.icgc.dcc.etl2.job.summarize.task;

import static org.icgc.dcc.common.core.model.FieldNames.PROJECT_ID;
import static org.icgc.dcc.etl2.core.job.FileType.PROJECT_SUMMARY;
import static org.icgc.dcc.etl2.core.util.ObjectNodes.textValue;
import static org.icgc.dcc.etl2.job.summarize.util.Projects.createDefaultProjectSummary;

import java.util.Map;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.icgc.dcc.common.core.model.FieldNames;
import org.icgc.dcc.etl2.core.job.FileType;
import org.icgc.dcc.etl2.core.task.GenericTask;
import org.icgc.dcc.etl2.core.task.TaskContext;
import org.icgc.dcc.etl2.core.task.TaskType;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Summarizes {@code Project} collection information.
 * <p>
 * Updates {@code Project} documents to be of the form:
 * 
 * <pre>
 * {
 *   ...
 *   "_summary" : {
 *     "_available_data_type" : [ "ssm", "cnsm" ],
 *     "_ssm1_tested_donor_count" : 1,
 *     "_cnsm_tested_donor_count" : 1,
 *     "_cngv_tested_donor_count" : 0,
 *     "_sgv_tested_donor_count" : 0,
 *     "_stgv_tested_donor_count" : 0,
 *     "_stsm_tested_donor_count" : 0,
 *     "_exp_seq_tested_donor_count" : 0,
 *     "_exp_array_tested_donor_count" : 0,
 *     "_meth_seq_tested_donor_count" : 0,
 *     "_meth_array_tested_donor_count" : 0,
 *     "_jcn_tested_donor_count" : 0,
 *     "_mirna_seq_tested_donor_count" : 0,
 *     "_pexp_tested_donor_count" : 0,
 *     "_total_donor_count" : 1,
 *     "_total_specimen_count" : 1,
 *     "_total_sample_count" : 1,
 *     "repository" : [ "EGA", "CGHub" ],     
 *     "experimental_analysis_performed" : {
 *       "WXS" : 1
 *     },     
 *   },
 *   ...
 * }
 * </pre>
 */
@RequiredArgsConstructor
public class ProjectSummarizeTask extends GenericTask {

  @NonNull
  private final Broadcast<Map<String, ObjectNode>> projectSummaryBroadcast;

  @Override
  public TaskType getType() {
    return TaskType.FILE_TYPE;
  }

  @Override
  public void execute(TaskContext taskContext) {
    val output = readProjects(taskContext)
        .map(joinProjectSummary(projectSummaryBroadcast));

    writeOutput(taskContext, output, PROJECT_SUMMARY);
  }

  private Function<ObjectNode, ObjectNode> joinProjectSummary(Broadcast<Map<String, ObjectNode>> projectSummaryBroadcast) {
    return project -> {
      Map<String, ObjectNode> projectSummaries = projectSummaryBroadcast.getValue();
      String projectName = textValue(project, PROJECT_ID);
      ObjectNode projectSummary = projectSummaries.get(projectName);
      ObjectNode summary = projectSummary == null ? createDefaultProjectSummary() : projectSummary;
      project.put(FieldNames.PROJECT_SUMMARY, summary);

      return project;
    };
  }

  private JavaRDD<ObjectNode> readProjects(TaskContext taskContext) {
    return readInput(taskContext, FileType.PROJECT);
  }

}
