/*
 * Copyright (c) 2014 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.release.job.index.io;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.propagate;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.elasticsearch.action.bulk.BulkProcessor.builder;
import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.unit.ByteSizeUnit.MB;
import static org.elasticsearch.common.xcontent.XContentType.SMILE;
import static org.icgc.dcc.common.core.util.FormatUtils.formatBytes;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.release.job.index.factory.JacksonFactory.newSmileWriter;

import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkProcessor.Listener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.icgc.dcc.release.core.document.Document;
import org.icgc.dcc.release.core.document.DocumentType;
import org.icgc.dcc.release.core.document.DocumentWriter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;

/**
 * Output destination for {@link DefaultDocument} instances to be written.
 */
@Slf4j
public class ElasticSearchDocumentWriter implements DocumentWriter {

  /**
   * Constants.
   */
  private static final int BULK_ACTIONS = -1; // Unlimited
  private static final ByteSizeValue BULK_SIZE = new ByteSizeValue(75, MB);
  private static final int SHUTDOWN_PERIOD_MINUTES = 60;
  private static final ObjectWriter BINARY_WRITER = newSmileWriter();
  private static final double TIMEOUT_MUTLIPLIPER = 1.3;
  private static final long DEFAULT_SLEEP_TIMEOUT = 5000L;
  private static final long DEFAULT_FAILED_EXECUTION_ID = -1L;
  private static final int MAX_FAILED_RETRIES = 5;

  /**
   * Meta data.
   */
  @Getter
  private final String indexName;
  private final DocumentType type;

  /**
   * Configuration.
   */
  private final int concurrentRequests;

  /**
   * Batching state.
   */
  private final BulkProcessor processor;
  private final AtomicInteger batchErrorCount = new AtomicInteger(0);
  private final Semaphore semaphore; // See https://github.com/elasticsearch/elasticsearch/issues/6314
  private long sleepTimeout = DEFAULT_SLEEP_TIMEOUT;
  private long failedExecutionId = DEFAULT_FAILED_EXECUTION_ID;
  private final boolean isCheckClusterState;

  /**
   * Dependencies.
   */
  private final Client client;

  /**
   * Status.
   */
  private int documentCount;
  @Getter
  private final AtomicInteger totalRetries = new AtomicInteger(0);

  public ElasticSearchDocumentWriter(Client client, String indexName, DocumentType type, int concurrentRequests,
      boolean isCheckClusterState) {
    this.indexName = indexName;
    this.type = type;
    this.concurrentRequests = concurrentRequests;
    this.processor = createProcessor(client);
    this.semaphore = new Semaphore(concurrentRequests);
    this.client = client;
    this.isCheckClusterState = isCheckClusterState;
  }

  @Override
  public void write(Document document) throws IOException {
    val request = createRequest(document.getId(), document.getSource());

    processor.add(request);
    documentCount++;
  }

  @Override
  @SneakyThrows
  public void close() throws IOException {
    log.info("Closing bulk processor...");
    processor.close();
    log.info("Finished closing bulk processor");

    log.info("Sleeping for 1 s...");
    SECONDS.sleep(1);

    val pendingCount = getPendingCount();
    if (pendingCount > 0) {
      log.info("Waiting up to {} mins for pending bulk requests...", SHUTDOWN_PERIOD_MINUTES, pendingCount);
      checkState(semaphore.tryAcquire(concurrentRequests, SHUTDOWN_PERIOD_MINUTES, MINUTES),
          "%s bulk requests failed to complete within the shutdown period", getPendingCount());
    }

    checkState(batchErrorCount.get() == 0, "Encountered %s batch errors when writing to ElasticSearch",
        batchErrorCount.get());

    log.info("Finished indexing {} '{}' documents", formatCount(documentCount), type.getName());
  }

  private int getPendingCount() {
    return concurrentRequests - semaphore.availablePermits();
  }

  private IndexRequest createRequest(String id, Object value) {
    return indexRequest(indexName)
        .type(type.getName())
        .id(id)
        .contentType(SMILE)
        .source(createSource(value));
  }

  private byte[] createSource(Object document) {
    try {
      return BINARY_WRITER.writeValueAsBytes(document);
    } catch (JsonProcessingException e) {
      propagate(e);
      return null;
    }
  }

  private BulkProcessor createProcessor(Client client) {
    return builder(client, createListener())
        .setBulkActions(BULK_ACTIONS)
        .setBulkSize(BULK_SIZE)
        .setConcurrentRequests(concurrentRequests)
        .build();
  }

  private void checkIfClusterGreen() {
    boolean isGreen = false;
    while (!isGreen) {
      log.info("Checking for cluster state before loading.");
      val health = client.admin().cluster().prepareHealth(indexName).execute().actionGet();

      if (health.getStatus() == ClusterHealthStatus.GREEN) {
        isGreen = true;
        sleepTimeout = DEFAULT_SLEEP_TIMEOUT;
      } else {
        log.warn("Cluster is {}. Sleeping", health.getStatus());
        sleep();
      }
    }
  }

  @SneakyThrows
  private void sleep() {
    Thread.sleep(sleepTimeout);
    sleepTimeout = Math.round(sleepTimeout * TIMEOUT_MUTLIPLIPER);
  }

  private boolean isRetryFailed() {
    return batchErrorCount.get() < MAX_FAILED_RETRIES;
  }

  /**
   * Blindly relies on the fact that this loader is single threaded.
   */
  private void resetFailedCount(long executionId) {
    if (executionId == failedExecutionId) {
      log.info("Successfully load failed index request '{}'", executionId);
      failedExecutionId = DEFAULT_FAILED_EXECUTION_ID;
      batchErrorCount.set(0);
    }
  }

  private Listener createListener() {
    return new BulkProcessor.Listener() {

      @Override
      @SneakyThrows
      public void beforeBulk(long executionId, BulkRequest request) {
        if (isCheckClusterState) {
          checkIfClusterGreen();
        }
        semaphore.acquire();

        val count = request.numberOfActions();
        val bytes = request.estimatedSizeInBytes();
        log.info("Sending '{}' bulk request with {} items ({} bytes)",
            new Object[] { type, formatCount(count), formatBytes(bytes) });
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
        semaphore.release();

        checkState(!response.hasFailures(), "Failed to index: %s", response.buildFailureMessage());
        resetFailedCount(executionId);
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
        log.info("Encountered exception during bulk load: ", failure);
        // Record errors for enclosing class
        batchErrorCount.incrementAndGet();
        totalRetries.incrementAndGet();

        semaphore.release();

        if (isRetryFailed()) {
          log.info("Flushing pending requests before retry...");
          processor.flush();
          log.info("Flushing finished. Retrying failed index request '{}'", executionId);
          processor.add(request);
          failedExecutionId = executionId;

          return;
        }

        log.error("Error performing bulk: ", failure);
        propagate(failure);
      }

    };
  }

}
