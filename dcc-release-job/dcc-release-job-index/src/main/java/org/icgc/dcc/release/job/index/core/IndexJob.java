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
package org.icgc.dcc.release.job.index.core;

import static java.lang.String.format;
import static org.icgc.dcc.release.job.index.factory.TransportClientFactory.newTransportClient;

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Map;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.release.core.config.SnpEffProperties;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.job.GenericJob;
import org.icgc.dcc.release.core.job.JobContext;
import org.icgc.dcc.release.core.job.JobType;
import org.icgc.dcc.release.core.task.Task;
import org.icgc.dcc.release.job.index.config.IndexProperties;
import org.icgc.dcc.release.job.index.core.IndexJobContext.IndexJobContextBuilder;
import org.icgc.dcc.release.job.index.model.BroadcastType;
import org.icgc.dcc.release.job.index.model.DocumentType;
import org.icgc.dcc.release.job.index.service.IndexService;
import org.icgc.dcc.release.job.index.task.CreateVCFFileTask;
import org.icgc.dcc.release.job.index.task.ResolveDonorsTask;
import org.icgc.dcc.release.job.index.task.ResolveGenesTask;
import org.icgc.dcc.release.job.index.task.ResolveProjectsTask;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

@Slf4j
@Component
@RequiredArgsConstructor(onConstructor = @__({ @Autowired }))
public class IndexJob extends GenericJob {

  /**
   * Dependencies.
   */
  @NonNull
  private final IndexProperties properties;
  @NonNull
  private final SnpEffProperties snpEffProperties;

  @Override
  public JobType getType() {
    return JobType.INDEX;
  }

  @Override
  public void execute(@NonNull JobContext jobContext) {
    // Cleanup output directories
    clean(jobContext);

    // TODO: Fix this to be tied to a run id:
    val indexName = resolveIndexName(jobContext.getReleaseName());
    if (properties.isSkipIndexing()) {
      write(jobContext, indexName);

      return;
    }

    //
    // TODO: Need to use spark.dynamicAllocation.enabled to dynamically increase memory for this job
    //
    // - http://spark.apache.org/docs/1.2.0/job-scheduling.html#dynamic-resource-allocation
    // - https://issues.apache.org/jira/browse/SPARK-4751
    //

    @Cleanup
    val client = newTransportClient(properties.getEsUri());
    @Cleanup
    val indexService = new IndexService(client);

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

  private void clean(JobContext jobContext) {
    delete(jobContext, resolveOutputFileTypes());
  }

  private static FileType[] resolveOutputFileTypes() {
    val outputFileTypes = Lists.<FileType> newArrayList();
    for (val documentType : DocumentType.values()) {
      outputFileTypes.add(documentType.getOutputFileType());
    }

    return outputFileTypes.toArray(new FileType[outputFileTypes.size()]);
  }

  static String resolveIndexName(String releaseName) {
    return "test-release-" + releaseName.toLowerCase();
  }

  private void write(JobContext jobContext, String indexName) {
    for (val task : createStreamingTasks(jobContext)) {
      jobContext.execute(task);
    }

    // Generate SSM VCF file
    if (properties.isExportVCF()) {
      jobContext.execute(new CreateVCFFileTask(snpEffProperties));
    }
  }

  @SneakyThrows
  private Collection<? extends Task> createStreamingTasks(JobContext jobContext) {
    val tasks = ImmutableList.<Task> builder();
    for (val documentType : DocumentType.values()) {
      val constructor = getConstructor(documentType);
      val indexJobContext = createIndexJobContext(jobContext, documentType);
      tasks.add((Task) constructor.newInstance(indexJobContext));
    }

    return tasks.build();
  }

  private static Constructor<?> getConstructor(DocumentType documentType) throws ClassNotFoundException,
      NoSuchMethodException {
    val indexClassName = documentType.getIndexClassName();
    val clazz = Class.forName(indexClassName);
    val constructor = clazz.getConstructor(IndexJobContext.class);

    return constructor;
  }

  private IndexJobContext createIndexJobContext(JobContext jobContext, DocumentType documentType) {
    val indexJobBuilder = IndexJobContext.builder()
        .esUri(properties.getEsUri())
        .indexName(resolveIndexName(jobContext.getReleaseName()))
        .skipIndexing(properties.isSkipIndexing());

    val indexJobDependencies = resolveDependencies(jobContext, documentType);
    setDependencies(indexJobBuilder, indexJobDependencies);

    return indexJobBuilder.build();
  }

  private static void setDependencies(IndexJobContextBuilder indexJobBuilder,
      Map<BroadcastType, ? extends Task> indexJobDependencies) {
    for (val entry : indexJobDependencies.entrySet()) {
      switch (entry.getKey()) {
      case PROJECT:
        val resolveProjectsTask = (ResolveProjectsTask) entry.getValue();
        indexJobBuilder.projectsBroadcast(resolveProjectsTask.getProjectsBroadcast());
        break;
      case DONOR:
        val resolveDonorsTask = (ResolveDonorsTask) entry.getValue();
        indexJobBuilder.donorsBroadcast(resolveDonorsTask.getDonorsBroadcast());
        break;
      case GENE:
        val resolveGenesTask = (ResolveGenesTask) entry.getValue();
        indexJobBuilder.genesBroadcast(resolveGenesTask.getGenesBroadcast());
        break;
      default:
        throw new IllegalArgumentException(format("Unrecoginzed broadcast type %s", entry.getKey()));
      }
    }
  }

  private static Map<BroadcastType, ? extends Task> resolveDependencies(JobContext jobContext, DocumentType documentType) {
    val tasksBuilder = ImmutableMap.<BroadcastType, Task> builder();
    for (val dependencyTask : documentType.getBroadcastDependencies()) {
      tasksBuilder.put(dependencyTask, createDependentyTask(dependencyTask, documentType));
    }

    val tasks = tasksBuilder.build();
    jobContext.execute(tasks.values());

    return tasks;
  }

  @SneakyThrows
  private static Task createDependentyTask(BroadcastType dependencyTask, DocumentType documentType) {
    val clazz = dependencyTask.getDependencyClass();
    val constructor = clazz.getConstructor(DocumentType.class);

    return constructor.newInstance(documentType);
  }

}