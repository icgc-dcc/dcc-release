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
import lombok.SneakyThrows;

import org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames;
import org.icgc.dcc.etl2.core.job.FileType;
import org.icgc.dcc.etl2.core.job.GenericJob;
import org.icgc.dcc.etl2.core.job.JobContext;
import org.icgc.dcc.etl2.core.job.JobType;
import org.icgc.dcc.etl2.job.orphan.task.OrphanTask;
import org.icgc.dcc.etl2.job.orphan.task.OrphanTaskDefinition;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableList;

@Component
public class OrphanJob extends GenericJob {

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
    clean(jobContext);
    markOrphans(jobContext);
  }

  private void clean(JobContext jobContext) {
    delete(jobContext,
        FileType.DONOR_ORPHANED,
        FileType.SPECIMEN_ORPHANED,
        FileType.SAMPLE_ORPHANED);
  }

  private void markOrphans(JobContext jobContext) {
    jobContext.execute(new OrphanTask(definitions));
  }

}
