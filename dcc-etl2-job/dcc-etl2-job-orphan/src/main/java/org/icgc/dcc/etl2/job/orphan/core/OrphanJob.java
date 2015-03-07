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
package org.icgc.dcc.etl2.job.orphan.core;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

import org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames;
import org.icgc.dcc.etl2.core.job.FileType;
import org.icgc.dcc.etl2.core.job.Job;
import org.icgc.dcc.etl2.core.job.JobContext;
import org.icgc.dcc.etl2.core.job.JobType;
import org.icgc.dcc.etl2.core.task.TaskExecutor;
import org.icgc.dcc.etl2.job.orphan.task.OrphanTask;
import org.icgc.dcc.etl2.job.orphan.task.OrphanTaskDefinition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableList;

@Component
@RequiredArgsConstructor(onConstructor = @__({ @Autowired }))
public class OrphanJob implements Job {

  /**
   * Dependencies.
   */
  @NonNull
  private final TaskExecutor executor;

  /**
   * Metadata.
   */
  private final Iterable<OrphanTaskDefinition> definitions = ImmutableList.of(
      new OrphanTaskDefinition(
          FileType.DONOR, FileType.DONOR_ORPHANED, SubmissionFieldNames.SUBMISSION_DONOR_ID),
      new OrphanTaskDefinition(
          FileType.SPECIMEN, FileType.SPECIMEN_ORPHANED, SubmissionFieldNames.SUBMISSION_SPECIMEN_ID),
      new OrphanTaskDefinition(
          FileType.SAMPLE, FileType.SAMPLE_ORPHANED, SubmissionFieldNames.SUBMISSION_ANALYZED_SAMPLE_ID)
      );

  @Override
  public JobType getType() {
    return JobType.ORPHAN;
  }

  @Override
  @SneakyThrows
  public void execute(@NonNull JobContext jobContext) {
    executor.execute(jobContext, new OrphanTask(definitions));
  }

}
