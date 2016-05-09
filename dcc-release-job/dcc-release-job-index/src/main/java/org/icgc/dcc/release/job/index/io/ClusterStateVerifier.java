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

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.propagate;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus.GREEN;

import java.util.concurrent.TimeUnit;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.client.Client;

@Slf4j
@RequiredArgsConstructor
public class ClusterStateVerifier {

  /**
   * Constants.
   */
  private static final int GET_HEALTH_STATUS_TIMEOUT_SECONDS = 20;
  private static final float TIMEOUT_MUTLIPLIPER = 1.3f;
  private static final int DEFAULT_SLEEP_TIMEOUT_SECONDS = 5;
  private static final int MAX_FAILED_RETRIES = 10;
  private static final int MAX_SLEEP_TIMEOUT_SECONDS = 600; // 10 mins

  private final Client client;
  private final String indexName;
  private final String id;
  private final IndexingState indexingState;

  public void ensureClusterState() {
    if (indexingState.isCheckClusterStateBeforeLoad()) {
      try {
        checkClusterState();
        log.debug("[{}] Consequent successful loads: {}", id, indexingState.getConsequentSuccessfulLoads());
        if (indexingState.isDisableClusterCheck()) {
          log.debug("[{}] Resetting consequent loads counter...", id);
          indexingState.resetConsequentSuccessfulLoads();
          indexingState.disableCheckClusterState();
        }
      } catch (ExhausedRetryException e) {
        indexingState.resetIndexState();
        throw propagate(e);
      }
    }
  }

  /**
   * Checks for the cluster's health. Blocks until the cluster is GREEN, but no more than
   * {@code MAX_SLEEP_TIMEOUT_SECONDS}.
   * @throws ExhausedRetryException
   */
  private void checkClusterState() {
    boolean isClusterGreen = false;
    int timeoutSecs = DEFAULT_SLEEP_TIMEOUT_SECONDS;

    // At some point of time the cluster will recover and we don't want to stop long running indexing.
    while (!isClusterGreen) {
      log.info("[{}] Checking for cluster state before loading.", id);
      val healthStatus = getHealthStatus();
      if (healthStatus == GREEN) {
        log.debug("[{}] Cluster is GREEN", id);
        isClusterGreen = true;
      } else {
        log.warn("[{}] Cluster is '{}'. Sleeping for {} seconds...", id, healthStatus, timeoutSecs);
        timeoutSecs = sleep(timeoutSecs);
      }
    }
  }

  /**
   * Get's the cluster's health status.<br>
   * Tries to get the cluster health status in {@code GET_HEALTH_STATUS_TIMEOUT_SECONDS} seconds. If the request fails
   * retries, but no more than {@code MAX_FAILED_RETRIES}. Max time interval between retries is
   * {@code MAX_SLEEP_TIMEOUT_SECONDS}.
   * @throws ExhausedRetryException
   */
  private ClusterHealthStatus getHealthStatus() {
    ClusterHealthStatus healthStatus = null;
    int availableRetries = MAX_FAILED_RETRIES;
    int timeoutSecs = DEFAULT_SLEEP_TIMEOUT_SECONDS;

    while (availableRetries-- > 0) {
      log.debug("[{}] Checking custer health status. Retries left: {}", id, availableRetries);
      try {
        healthStatus = getClusterHealthStatus();
        log.debug("[{}] Cluster health object: {}", id, healthStatus);
      } catch (ElasticsearchException e) {
        val retryCount = MAX_FAILED_RETRIES - availableRetries;
        log.warn("[{}][{}/{}] Failed to check cluster health. Retrying because of exception: {}", id, retryCount,
            MAX_FAILED_RETRIES, e);
        log.info("[{}] Sleeping for {} seconds...", id, timeoutSecs);
        timeoutSecs = sleep(timeoutSecs);
      }
    }

    // No more retries and no health status
    if (healthStatus == null) {
      log.warn("Failed to check cluster health in '{}' attempts. Exiting...", MAX_FAILED_RETRIES);
      throw new ExhausedRetryException();
    }

    checkState(availableRetries != 0, "Cluster healthcheck should failed but instead it's equals to %s", healthStatus);

    return healthStatus;
  }

  /**
   * Sleeps for {@code timeoutSeconds} and return next sleep timeout.
   */
  @SneakyThrows
  private static int sleep(int timeoutSeconds) {
    SECONDS.sleep(timeoutSeconds);
    val nextTimeout = Math.round(timeoutSeconds * TIMEOUT_MUTLIPLIPER);

    return nextTimeout > MAX_SLEEP_TIMEOUT_SECONDS ? MAX_SLEEP_TIMEOUT_SECONDS : nextTimeout;
  }

  private ClusterHealthStatus getClusterHealthStatus() {
    return client
        .admin()
        .cluster()
        .prepareHealth(indexName)
        .execute()
        .actionGet(GET_HEALTH_STATUS_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .getStatus();
  }

}
