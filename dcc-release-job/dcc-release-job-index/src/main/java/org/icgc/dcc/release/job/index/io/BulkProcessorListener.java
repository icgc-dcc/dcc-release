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
package org.icgc.dcc.release.job.index.io;

import static org.icgc.dcc.common.core.util.Formats.formatBytes;
import static org.icgc.dcc.common.core.util.Formats.formatCount;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkProcessor.Listener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;

import com.google.common.base.Throwables;

@Slf4j
@RequiredArgsConstructor
public class BulkProcessorListener implements Listener {

  @NonNull
  private final ClusterStateVerifier clusterStateVerifier;
  @NonNull
  private final IndexingState indexingState;
  @NonNull
  private final String writerId;

  @Setter
  private BulkProcessor processor;

  @Override
  public void beforeBulk(long executionId, BulkRequest request) {
    clusterStateVerifier.ensureClusterState();
    log.debug("{}", indexingState);
    indexingState.startIndexing();
    printRequestStats(executionId, request);
  }

  @Override
  public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
    log.debug("[{}] Received successful response for request {}", writerId, executionId);
    log.debug("{}", indexingState);

    // Unsuccessful bulk response. Re-index only failed requests.
    if (response.hasFailures()) {
      log.warn("[{}] Encountered exceptions during bulk load: {}", writerId,
          getExceptionMessage(response.buildFailureMessage()));
      indexingState.enableCheckClusterState();
      reindexFailedRequests(processor, request, response);
    }

    // Successful bulk response
    log.info("[{}] Successfully loaded bulk request '{}'.", writerId, executionId);
    indexingState.resetIndexState();
    log.debug("{}", indexingState);
  }

  @Override
  public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
    log.debug("[{}] Received unsuccessful response for request {}", writerId, executionId);
    indexingState.enableCheckClusterState();
    log.debug("{}", indexingState);

    log.warn("[{}] Encountered exception during bulk load: {}", writerId, getExceptionMessage(failure));
    retryRequest(executionId, request);
    log.debug("{}", indexingState);

  }

  private void retryRequest(long executionId, BulkRequest request) {
    log.debug("[{}] Retrying request {}", writerId, executionId);
    indexingState.checkRetryFailed();

    log.info("[{}] Retrying failed index request '{}'", writerId, executionId);
    indexingState.incrementTotalRetries();
    indexingState.incrementRetries();
    reindexBulkRequest(processor, request);
  }

  private static void reindexBulkRequest(BulkProcessor processor, BulkRequest bulkRequest) {
    for (ActionRequest<?> request : bulkRequest.requests()) {
      processor.add(request);
    }
    processor.flush();
  }

  private void printRequestStats(long executionId, BulkRequest request) {
    val count = request.numberOfActions();
    val bytes = request.estimatedSizeInBytes();
    log.info("[{}] Sending bulk request '{}' with {} items ({} bytes)", writerId, executionId, formatCount(count),
        formatBytes(bytes));
  }

  private static String getExceptionMessage(Throwable failure) {
    val message = Throwables.getStackTraceAsString(failure);

    return getExceptionMessage(message);
  }

  private static String getExceptionMessage(String message) {
    val maxChars = 1000;

    return message.length() > maxChars ? message.substring(0, maxChars) : message;
  }

  @SuppressWarnings("rawtypes")
  private void reindexFailedRequests(BulkProcessor processor, BulkRequest bulkRequest, BulkResponse bulkResponse) {
    log.debug("[{}] Re-indexing failed requests", writerId);

    val requests = bulkRequest.requests();
    val iterator = bulkResponse.iterator();

    while (iterator.hasNext()) {
      val response = iterator.next();
      if (response.isFailed()) {
        val failedRequest = requests.get(response.getItemId());
        processor.add(failedRequest);
      }
    }

    log.debug("[{}] Flushing failed requests...", writerId);
    processor.flush();
    log.debug("[{}] Finished failed requests re-indexing...", writerId);
  }

}
