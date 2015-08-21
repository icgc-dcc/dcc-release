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
package org.icgc.dcc.etl2.job.summarize.core;

import static org.icgc.dcc.etl2.core.util.Stopwatches.createStarted;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl2.core.job.FileType;
import org.icgc.dcc.etl2.core.job.GenericJob;
import org.icgc.dcc.etl2.core.job.JobContext;
import org.icgc.dcc.etl2.core.job.JobType;
import org.icgc.dcc.etl2.job.summarize.task.DonorSummarizeTask;
import org.icgc.dcc.etl2.job.summarize.task.FeatureTypeSummarizeTask;
import org.icgc.dcc.etl2.job.summarize.task.GeneSetSummarizeTask;
import org.icgc.dcc.etl2.job.summarize.task.GeneSummarizeTask;
import org.icgc.dcc.etl2.job.summarize.task.ObservationSummarizeTask;
import org.icgc.dcc.etl2.job.summarize.task.ProjectSummarizeTask;
import org.icgc.dcc.etl2.job.summarize.task.ReleaseSummarizeTask;
import org.icgc.dcc.etl2.job.summarize.task.ResolveGeneSummaryTask;
import org.icgc.dcc.etl2.job.summarize.task.ResolveProjectSummaryTask;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class SummarizeJob extends GenericJob {

  private static final FileType[] OUTPUT_FILE_TYPES = {
      FileType.DONOR_GENE_OBSERVATION_SUMMARY,
      FileType.GENE_SET_SUMMARY,
      FileType.GENE_SUMMARY,
      FileType.PROJECT_SUMMARY,
      FileType.OBSERVATION_SUMMARY,
      FileType.RELEASE_SUMMARY };

  @Override
  public JobType getType() {
    return JobType.SUMMARIZE;
  }

  @Override
  @SneakyThrows
  public void execute(@NonNull JobContext jobContext) {
    clean(jobContext);
    summarize(jobContext);
  }

  private void clean(JobContext jobContext) {
    delete(jobContext, OUTPUT_FILE_TYPES);
  }

  private static void summarize(JobContext jobContext) {
    val watch = createStarted();
    log.info("Executing summary job...");
    jobContext.execute(new GeneSetSummarizeTask());

    val featureTypeSummary = new FeatureTypeSummarizeTask();
    jobContext.execute(featureTypeSummary);
    val donorSummarizeTask = new DonorSummarizeTask(featureTypeSummary.getProjectDonorSummary());
    jobContext.execute(donorSummarizeTask);

    val resolveProjectSummaryTask = new ResolveProjectSummaryTask();
    jobContext.execute(resolveProjectSummaryTask);
    jobContext.execute(new ProjectSummarizeTask(resolveProjectSummaryTask.getProjectSummaryBroadcast()));

    val resolveGeneStatsTask = new ResolveGeneSummaryTask();
    jobContext.execute(resolveGeneStatsTask);
    jobContext.execute(
        new GeneSummarizeTask(resolveGeneStatsTask.getGeneDonorTypeCounts()),
        new ObservationSummarizeTask());
    jobContext.execute(new ReleaseSummarizeTask(donorSummarizeTask.getDonorsCount(), donorSummarizeTask
        .getLiveDonorsCount()));
    log.info("Finished executing summary job in {}", watch);
  }

}
