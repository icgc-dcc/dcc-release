/*
 * Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.                             
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

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.copyOf;
import static com.google.common.collect.Sets.difference;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;
import static org.icgc.dcc.release.core.document.DocumentType.DONOR_CENTRIC_TYPE;
import static org.icgc.dcc.release.job.index.factory.TransportClientFactory.newTransportClient;
import static org.icgc.dcc.release.job.index.utils.IndexTasks.getBigFilesPath;
import static org.icgc.dcc.release.job.index.utils.IndexTasks.getEsExportPath;
import static org.icgc.dcc.release.job.index.utils.IndexTasks.getIndexName;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.release.core.document.DocumentType;
import org.icgc.dcc.release.core.job.GenericJob;
import org.icgc.dcc.release.core.job.JobContext;
import org.icgc.dcc.release.core.job.JobType;
import org.icgc.dcc.release.core.task.DeleteFileTask;
import org.icgc.dcc.release.core.task.Task;
import org.icgc.dcc.release.job.index.config.IndexProperties;
import org.icgc.dcc.release.job.index.service.IndexService;
import org.icgc.dcc.release.job.index.service.IndexVerificationService;
import org.icgc.dcc.release.job.index.task.EsExportTask;
import org.icgc.dcc.release.job.index.task.IndexBigFilesTask;
import org.icgc.dcc.release.job.index.task.IndexTask;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
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

  @Override
  public JobType getType() {
    return JobType.INDEX;
  }

  @Override
  public void execute(JobContext jobContext) {
    clean(jobContext);

    val indexName = getIndexName(jobContext.getReleaseName());
    val indexTypes = getIndexTypes();
    val allTasks = createTasks(indexName, indexTypes);
    log.info("Created {} tasks.", allTasks.size());

    if (allTasks.isEmpty()) {
      log.info("No tasks to execute. Finishing IndexJob...");
      return;
    }

    if (!hasIndexTasks(allTasks)) {
      log.info("IndexJob doesn't have indexing tasks. Creating Elasticsearch archives only...");
      jobContext.execute(allTasks);
      return;
    }

    @Cleanup
    val client = newTransportClient(properties.getEsUri());
    @Cleanup
    val indexService = new IndexService(client);

    prepareIndex(indexName, indexService, indexTypes);
    val noBigFilesTasks = allTasks.stream()
        .filter(task -> !(task instanceof IndexBigFilesTask))
        .collect(toImmutableList());
    if (!noBigFilesTasks.isEmpty()) {
      jobContext.execute(noBigFilesTasks);
    }

    val bigFilesTask = allTasks.stream()
        .filter(task -> task instanceof IndexBigFilesTask)
        .findFirst();
    if (bigFilesTask.isPresent()) {
      log.info("Indexing big files...");
      jobContext.execute(bigFilesTask.get());
    }

    // Report
    log.info("Reporting index...");
    indexService.reportIndex(indexName);

    // Compact
    log.info("Optimizing index...");
    indexService.optimizeIndex(indexName);
    indexService.optimizeForSearching(indexName);

    // Freeze
    log.info("Freezing index...");
    indexService.freezeIndex(indexName);

    log.info("Verifying index...");
    val verificationService = new IndexVerificationService(client, indexName);
    verificationService.verify();
  }

  Collection<Task> createTasks(String indexName, Set<DocumentType> indexTypes) {
    val tasks = ImmutableList.<Task> builder();
    if (properties.isIndexDocuments()) {
      log.info("Creating index tasks for index types: {}", indexTypes);
      tasks.addAll(createIndexTasks(indexName, indexTypes));
    }

    if (properties.isExportEsIndex()) {
      log.info("Creating export Elasticsearch index tasks...");
      tasks.addAll(createEsExportTasks(indexName, indexTypes));
    }

    return tasks.build();
  }

  private void prepareIndex(String indexName, IndexService indexService, Set<DocumentType> indexTypes) {
    log.info("Initializing index...");
    if (isIndexAll()) {
      indexService.initializeIndex(indexName, indexTypes);
    } else {
      log.info("Unfreezing index because of indexing of big documents only...");
      indexService.unfreezeIndex(indexName);
    }
    indexService.optimizeForIndexing(indexName);
  }

  private void clean(JobContext jobContext) {
    val workingDir = jobContext.getWorkingDir();
    val cleanupTasks = Lists.<Task> newArrayList();

    if (properties.isIndexDocuments() && !properties.isBigDocumentsOnly()) {
      val bigDocsPath = getBigFilesPath(workingDir);
      cleanupTasks.add(new DeleteFileTask(bigDocsPath));
      log.info("Prepared clean big documents directory task.");
    }

    if (properties.isExportEsIndex()) {
      val esExportPath = getEsExportPath(workingDir);
      cleanupTasks.add(new DeleteFileTask(esExportPath));
      log.info("Prepared clean Elasticsearch export directory task.");
    }

    if (!cleanupTasks.isEmpty()) {
      jobContext.execute(cleanupTasks);
    }
  }

  private boolean isIndexAll() {
    return properties.isBigDocumentsOnly() == false;
  }

  private Collection<? extends Task> createEsExportTasks(String indexName, Set<DocumentType> indexTypes) {
    return indexTypes.stream()
        .map(dt -> new EsExportTask(indexName, dt))
        .collect(toImmutableList());
  }

  @SneakyThrows
  private Collection<Task> createIndexTasks(final String indexName, Set<DocumentType> indexTypes) {
    val indexTasks = ImmutableList.<Task> builder();
    indexTasks.add(new IndexBigFilesTask(properties.getEsUri()));

    if (!properties.isBigDocumentsOnly()) {
      log.info("Big indexing documents only. Skip the rest index tasks creation...");
      for (val indexType : indexTypes) {
        indexTasks.add(createIndexTask(indexName, indexType));
      }
    }

    return indexTasks.build();
  }

  private IndexTask createIndexTask(String indexName, DocumentType documentType) {
    return new IndexTask(properties.getEsUri(), indexName, documentType, properties.getBigDocumentThresholdMb());
  }

  private Set<DocumentType> getIndexTypes() {
    if (properties.isBigDocumentsOnly()) {
      log.info("Indexing big documents only. Setting index types to {}...", DONOR_CENTRIC_TYPE);
      return Collections.singleton(DONOR_CENTRIC_TYPE);
    }

    val includeTypes = properties.getIncludeTypes();
    val excludeTypes = properties.getExcludeTypes();
    checkState(includeTypes.isEmpty() || excludeTypes.isEmpty(), "Indices can be either included, or excluded.");

    // All types
    if (includeTypes.isEmpty() && excludeTypes.isEmpty()) {
      return ImmutableSet.copyOf(DocumentType.values());
    }

    // Includes
    if (includeTypes.isEmpty() == false) {
      return includeTypes.stream()
          .map(type -> DocumentType.valueOf(type.toUpperCase()))
          .collect(toImmutableSet());
    }

    // Excludes
    val excludeDocumentTypes = excludeTypes.stream()
        .map(type -> DocumentType.valueOf(type.toUpperCase()))
        .collect(toImmutableSet());

    return difference(copyOf(DocumentType.values()), excludeDocumentTypes);
  }

  private static boolean hasIndexTasks(Collection<Task> tasks) {
    return tasks.stream()
        .anyMatch(task -> task instanceof IndexTask || task instanceof IndexBigFilesTask);
  }

}
