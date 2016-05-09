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

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IndexingState {

  /**
   * Constants.
   */
  private static final int MAX_FAILED_RETRIES = 10;
  private static final int MAX_CONSEQUENT_SUCCESSFUL_LOADS = 5;

  /**
   * State.
   */
  @Getter
  // Might be used for statistics later.
  private final AtomicInteger totalRetries = new AtomicInteger(0);
  // A flag that indicates that a bulk load is in progress.
  private final AtomicInteger pendingBulkRequest = new AtomicInteger(0);
  private final AtomicInteger batchRetryCount = new AtomicInteger(0);
  private final AtomicBoolean checkClusterStateBeforeLoad = new AtomicBoolean();
  private final AtomicInteger consequentSuccessfulLoads = new AtomicInteger(0);

  private final String id;

  public IndexingState(String id) {
    this.id = id;
  }

  /**
   * Checks if the retries are not exhausted yet.
   * @throws ExhausedRetryException
   */
  public void checkRetryFailed() {
    if (!canRetryIndexing()) {
      log.warn("[{}] Exhausted retries. Giving up...", id);
      throw new ExhausedRetryException();
    }
  }

  /**
   * Cleans {@code pendingBulkRequest} and {@code batchErrorCount}.
   */
  public void resetIndexState() {
    // TODO: check if we should decrement instead of resetting.
    resetPendingRequests();
    resetRetries();

    // Still checking cluster state before each load?
    if (checkClusterStateBeforeLoad.get()) {
      consequentSuccessfulLoads.incrementAndGet();
    }
  }

  public void resetConsequentSuccessfulLoads() {
    consequentSuccessfulLoads.set(0);
  }

  public int getConsequentSuccessfulLoads() {
    return consequentSuccessfulLoads.get();
  }

  public boolean isDisableClusterCheck() {
    return consequentSuccessfulLoads.get() >= MAX_CONSEQUENT_SUCCESSFUL_LOADS;
  }

  public boolean isCheckClusterStateBeforeLoad() {
    return checkClusterStateBeforeLoad.get() == true;
  }

  public void enableCheckClusterState() {
    checkClusterStateBeforeLoad.set(true);
  }

  public void disableCheckClusterState() {
    checkClusterStateBeforeLoad.set(false);
  }

  public void incrementTotalRetries() {
    totalRetries.incrementAndGet();
  }

  public boolean hasPendingRequests() {
    return pendingBulkRequest.get() != 0;
  }

  public int getPendingRequestsCount() {
    return pendingBulkRequest.get();
  }

  public void resetPendingRequests() {
    pendingBulkRequest.set(0);
  }

  public void startIndexing() {
    pendingBulkRequest.incrementAndGet();
  }

  public void incrementRetries() {
    batchRetryCount.incrementAndGet();
  }

  public void resetRetries() {
    batchRetryCount.set(0);
  }

  public int getRetries() {
    return batchRetryCount.get();
  }

  public boolean canRetryIndexing() {
    return batchRetryCount.get() < MAX_FAILED_RETRIES;
  }

  @Override
  public String toString() {
    return format("Pending requests: %s, indexing retries: %s", getPendingRequestsCount(), getRetries());
  }

  @SneakyThrows
  public void waitForPendingRequests() {
    val timeoutSeconds = 5;
    val requestsPerMinute = 60 / timeoutSeconds;

    // Wait for 15 minutes before fail
    val pendingRequestTimeoutMins = 15;
    int retriesLeft = requestsPerMinute * pendingRequestTimeoutMins;
    while (hasPendingRequests()) {
      log.info("[{}] The processor has {} pending requests. Waiting for 5 secs...", id,
          getPendingRequestsCount());
      SECONDS.sleep(timeoutSeconds);

      retriesLeft--;
      if (retriesLeft <= 0) {
        log.error("Tired of waiting for the pending requests after {} mins. Killing myself...",
            pendingRequestTimeoutMins);
        throw new ExhausedRetryException();
      }
    }
  }

}
