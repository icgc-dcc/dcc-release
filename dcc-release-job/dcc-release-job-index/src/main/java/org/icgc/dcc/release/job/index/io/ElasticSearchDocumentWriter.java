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

import static com.google.common.base.Throwables.propagate;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus.GREEN;
import static org.elasticsearch.action.bulk.BulkProcessor.builder;
import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.unit.ByteSizeUnit.MB;
import static org.elasticsearch.common.xcontent.XContentType.SMILE;
import static org.icgc.dcc.common.core.util.FormatUtils.formatBytes;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.release.job.index.factory.JacksonFactory.newSmileWriter;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionRequest;
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
  private static final ObjectWriter BINARY_WRITER = newSmileWriter();
  private static final double TIMEOUT_MUTLIPLIPER = 1.3;
  private static final long DEFAULT_SLEEP_TIMEOUT = 5000L;
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
  private final String writerId = createWriterId();

  /**
   * Batching state.
   */
  private final BulkProcessor processor;

  /**
   * A flag that indicates that a bulk load is in progress.
   */
  private final AtomicInteger pendingBulkRequest = new AtomicInteger(0);
  private final AtomicInteger batchRetryCount = new AtomicInteger(0);
  private long sleepTimeout = DEFAULT_SLEEP_TIMEOUT;
  private final boolean isCheckClusterStateBeforeLoad;

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

  public ElasticSearchDocumentWriter(Client client, String indexName, DocumentType type, boolean isCheckClusterState) {
    this.indexName = indexName;
    this.type = type;
    this.processor = createProcessor(client);
    this.client = client;
    this.isCheckClusterStateBeforeLoad = isCheckClusterState;
    log.info("[{}] Created ES document writer.", writerId);
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
    // Initiate an index request which will set the pendingBulkRequest
    processor.flush();

    log.info("[{}] Closing bulk processor...", writerId);
    waitForPendingRequests();
    processor.close();
    log.info("[{}] Finished indexing {} '{}' documents", writerId, formatCount(documentCount), type.getName());
  }

  @SneakyThrows
  private void waitForPendingRequests() {
    while (pendingBulkRequest.get() != 0) {
      log.info("[{}] The processor has pending requests. Waiting for 5 secs...", writerId);
      SECONDS.sleep(5);
    }
  }

  private IndexRequest createRequest(String id, Object value) {
    return indexRequest(indexName)
        .type(type.getName())
        .id(id)
        .contentType(SMILE)
        .source(createSource(value));
  }

  private BulkProcessor createProcessor(Client client) {
    return builder(client, createListener())
        .setBulkActions(BULK_ACTIONS)
        .setBulkSize(BULK_SIZE)
        .setConcurrentRequests(0)
        .build();
  }

  /**
   * @throws ExhausedRetryException
   */
  private void checkClusterState() {
    boolean isClusterGreen = false;

    while (!isClusterGreen) {
      log.info("[{}] Checking for cluster state before loading.", writerId);
      val healthStatus = getHealthStatus(client, indexName);

      if (healthStatus == GREEN) {
        isClusterGreen = true;
        sleepTimeout = DEFAULT_SLEEP_TIMEOUT;
      } else {
        log.warn("[{}] Cluster is '{}'. Sleeping...", writerId, healthStatus);
        sleep();
      }
    }
  }

  @SneakyThrows
  private void sleep() {
    Thread.sleep(sleepTimeout);
    sleepTimeout = Math.round(sleepTimeout * TIMEOUT_MUTLIPLIPER);
  }

  /**
   * Checks if the retries are not exhausted yet.
   * @throws ExhausedRetryException
   */
  private void checkRetryFailed() {
    if (batchRetryCount.get() > MAX_FAILED_RETRIES) {
      log.warn("[{}] Exhausted retries. Giving up...", writerId);
      throw new ExhausedRetryException();
    }
  }

  /**
   * Cleans {@code pendingBulkRequest} and {@code batchErrorCount}.
   */
  private void resetIndexState() {
    pendingBulkRequest.set(0);
    batchRetryCount.set(0);
  }

  private void retryRequest(long executionId, BulkRequest request) {
    checkRetryFailed();

    if (isCheckClusterStateBeforeLoad) {
      checkClusterState();
    }

    log.info("[{}] Retrying failed index request '{}'", writerId, executionId);
    totalRetries.incrementAndGet();
    batchRetryCount.incrementAndGet();
    reindexBulkRequest(processor, request);
  }

  private void printRequestStats(long executionId, BulkRequest request) {
    val count = request.numberOfActions();
    val bytes = request.estimatedSizeInBytes();
    log.info("[{}] Sending '{}' bulk request '{}' with {} items ({} bytes)",
        writerId, type, executionId, formatCount(count), formatBytes(bytes));
  }

  private static void reindexBulkRequest(BulkProcessor processor, BulkRequest bulkRequest) {
    for (ActionRequest<?> request : bulkRequest.requests()) {
      processor.add(request);
    }
    processor.flush();
  }

  /**
   * @throws ExhausedRetryException
   */
  private static ClusterHealthStatus getHealthStatus(Client client, String indexName) {
    ClusterHealthStatus healthStatus = null;
    int availableRetries = MAX_FAILED_RETRIES;

    while (availableRetries-- > 0) {
      try {
        healthStatus = getClusterHealthStatus(client, indexName);
      } catch (ElasticsearchException e) {
        log.warn("{}", e);
      }
    }

    if (healthStatus == null) {
      throw new ExhausedRetryException();
    }

    return healthStatus;
  }

  private static ClusterHealthStatus getClusterHealthStatus(Client client, String indexName) {
    return client
        .admin()
        .cluster()
        .prepareHealth(indexName)
        .execute()
        .actionGet()
        .getStatus();
  }

  private static String createWriterId() {
    val id = new Random().nextInt();
    return String.valueOf(Math.abs(id));
  }

  @SuppressWarnings("rawtypes")
  private static void reindexFailedRequests(BulkProcessor processor, BulkRequest bulkRequest, BulkResponse bulkResponse) {
    val requests = bulkRequest.requests();
    val iterator = bulkResponse.iterator();

    while (iterator.hasNext()) {
      val response = iterator.next();
      if (response.isFailed()) {
        val request = requests.get(response.getItemId());
        processor.add(request);
      }
    }

    processor.flush();
  }

  private static byte[] createSource(Object document) {
    try {
      return BINARY_WRITER.writeValueAsBytes(document);
    } catch (JsonProcessingException e) {
      propagate(e);
      return null;
    }
  }

  private Listener createListener() {
    return new BulkProcessor.Listener() {

      @Override
      public void beforeBulk(long executionId, BulkRequest request) {
        pendingBulkRequest.incrementAndGet();
        printRequestStats(executionId, request);
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
        // Unsuccessful bulk response. Re-index only failed requests.
        if (response.hasFailures()) {
          log.info("[{}] Encountered exceptions during bulk load: ", writerId, response.buildFailureMessage());
          reindexFailedRequests(processor, request, response);
        }

        // Successful bulk response
        log.info("[{}] Successfully loaded bulk request '{}'.", writerId, executionId);
        resetIndexState();
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
        // Exhausted retries. Abort indexing.
        if (failure instanceof ExhausedRetryException) {
          propagate(failure);
        }

        log.info("[{}] Encountered exception during bulk load: ", writerId, failure);
        retryRequest(executionId, request);
      }

    };
  }

  private static class ExhausedRetryException extends RuntimeException {}

}
