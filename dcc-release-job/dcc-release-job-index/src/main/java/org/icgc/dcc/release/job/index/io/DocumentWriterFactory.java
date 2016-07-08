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

import static lombok.AccessLevel.PRIVATE;
import static org.elasticsearch.action.bulk.BulkProcessor.builder;

import java.util.Random;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.val;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.Client;
import org.icgc.dcc.release.core.document.DocumentType;
import org.icgc.dcc.release.core.document.DocumentWriter;
import org.icgc.dcc.release.job.index.factory.TransportClientFactory;

@NoArgsConstructor(access = PRIVATE)
public final class DocumentWriterFactory {

  private static final Random RANDOM_GENERATOR = new Random();
  private static final int BULK_ACTIONS = -1; // Unlimited

  public static DocumentWriter createDocumentWriter(@NonNull String esUri, @NonNull String indexName,
      @NonNull DocumentType type) {
    val context = DocumentWriterContext.builder()
        .esUri(esUri)
        .indexName(indexName)
        .build();

    return createDocumentWriter(context);
  }

  public static DocumentWriter createDocumentWriter(@NonNull Client client, @NonNull String indexName,
      @NonNull DocumentType type) {
    val context = DocumentWriterContext.builder()
        .indexName(indexName)
        .build();

    return createDocumentWriter(client, context);
  }

  public static DocumentWriter createDocumentWriter(@NonNull DocumentWriterContext context) {
    val client = TransportClientFactory.newTransportClient(context.getEsUri(), true);

    return createDocumentWriter(client, context);
  }

  private static DocumentWriter createDocumentWriter(@NonNull Client client, @NonNull DocumentWriterContext context) {
    val writerId = createWriterId();
    val indexingState = new IndexingState(writerId);
    val clusterStateVerifier = new ClusterStateVerifier(client, context.getIndexName(), writerId, indexingState);
    val bulkProcessorListener = new BulkProcessorListener(clusterStateVerifier, indexingState, writerId);
    val bulkProcessor = createProcessor(client, bulkProcessorListener);

    return new ElasticSearchDocumentWriter(
        client,
        context.getIndexName(),
        indexingState,
        bulkProcessor,
        writerId);
  }

  public static DocumentWriter createFilteringDocumentWriter(@NonNull DocumentWriterContext context) {
    val client = TransportClientFactory.newTransportClient(context.getEsUri(), true);
    val writerId = createWriterId();
    val indexingState = new IndexingState(writerId);
    val clusterStateVerifier = new ClusterStateVerifier(client, context.getIndexName(), writerId, indexingState);
    val bulkProcessorListener = new BulkProcessorListener(clusterStateVerifier, indexingState, writerId);
    val bulkProcessor = createProcessor(client, bulkProcessorListener);

    return new FilteringElasticSearchDocumentWriter(client, context.getIndexName(), indexingState,
        clusterStateVerifier, bulkProcessor, writerId, context.getDocumentThreshold(), context.getWorkingDir(),
        context.getFsSettings());
  }

  private static String createWriterId() {
    val id = RANDOM_GENERATOR.nextInt(Integer.MAX_VALUE);

    return String.valueOf(Math.abs(id));
  }

  private static BulkProcessor createProcessor(Client client, BulkProcessorListener listener) {
    val bulkProcessor = builder(client, listener)
        .setBulkActions(BULK_ACTIONS)
        .setBulkSize(ElasticSearchDocumentWriter.BULK_SIZE)
        .setConcurrentRequests(0)
        .build();

    // Need to give back reference to bulkProcessor as it's reused for re-indexing of failed requests.
    listener.setProcessor(bulkProcessor);

    return bulkProcessor;
  }

}
