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
package org.icgc.dcc.etl2.job.index.core;

import static org.icgc.dcc.etl2.job.index.factory.TransportClientFactory.newTransportClient;

import java.util.Collection;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl2.core.job.Job;
import org.icgc.dcc.etl2.core.job.JobContext;
import org.icgc.dcc.etl2.core.job.JobType;
import org.icgc.dcc.etl2.core.task.Task;
import org.icgc.dcc.etl2.core.util.Streams;
import org.icgc.dcc.etl2.job.index.config.IndexProperties;
import org.icgc.dcc.etl2.job.index.model.DocumentType;
import org.icgc.dcc.etl2.job.index.service.IndexService;
import org.icgc.dcc.etl2.job.index.task.MutationCentricIndexTask;
import org.icgc.dcc.etl2.job.index.task.RemoteIndexTask;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableList;

@Slf4j
@Component
@RequiredArgsConstructor(onConstructor = @__({ @Autowired }))
public class IndexJob implements Job {

  /**
   * Dependencies.
   */
  @NonNull
  private final IndexProperties properties;

  @Override
  public JobType getType() {
    return JobType.INDEX;
  }

  @Override
  public void execute(@NonNull JobContext jobContext) {

    //
    // TODO: Need to use spark.dynamicAllocation.enabled to dynamically increase memory for this job
    //
    // - http://spark.apache.org/docs/1.2.0/job-scheduling.html#dynamic-resource-allocation
    // - https://issues.apache.org/jira/browse/SPARK-4751
    //

    @Cleanup
    val client = newTransportClient(properties.getEsUri());
    val indexService = new IndexService(client);

    // TODOD: Fix this to be tied to a run id:
    val indexName = "test-etl2-" + jobContext.getReleaseName().toLowerCase();

    // Prepare
    log.info("Initializing index...");
    indexService.initializeIndex(indexName);

    // Populate
    log.info("Populating index...");
    write(jobContext, indexName);

    // Report
    log.info("Reporting index...");
    indexService.reportIndex(indexName);

    // Compact
    log.info("Optimizing index...");
    indexService.optimizeIndex(indexName);

    // Freeze
    log.info("Freezing index...");
    indexService.freezeIndex(indexName);
  }

  private void write(JobContext jobContext, String indexName) {
    val tasks = createStreamingTasks(jobContext, indexName);

    jobContext.execute(tasks);
  }

  private Collection<? extends Task> createStreamingTasks(JobContext jobContext, String indexName) {
    return ImmutableList.of(
        new MutationCentricIndexTask()
        // , new GeneCentricIndexTask()
        // , new DonorCentricIndexTask()
        );
  }

  @SuppressWarnings("unused")
  private Collection<? extends Task> createRemoteTasks(JobContext jobContext, String indexName) {
    return Streams.map(DocumentType.values(), type -> {
      return new RemoteIndexTask(properties, indexName, jobContext.getReleaseName(), type);
    });
  }

}