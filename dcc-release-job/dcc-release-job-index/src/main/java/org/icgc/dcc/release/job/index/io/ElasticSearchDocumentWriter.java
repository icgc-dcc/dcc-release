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
import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.unit.ByteSizeUnit.MB;
import static org.elasticsearch.common.xcontent.XContentType.SMILE;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;

import java.io.IOException;

import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.icgc.dcc.release.core.document.Document;
import org.icgc.dcc.release.core.document.DocumentType;
import org.icgc.dcc.release.core.document.DocumentWriter;
import org.icgc.dcc.release.core.util.JacksonFactory;

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
  public static final ByteSizeValue BULK_SIZE = new ByteSizeValue(36, MB);
  private static final ObjectWriter BINARY_WRITER = JacksonFactory.DEFAULT_SMILE_WRITER;

  /**
   * Meta data.
   */
  @Getter
  private final String indexName;

  /**
   * Helps to track log records related to this particular writer.
   */
  private final String writerId;

  /**
   * Batching state.
   */
  @Getter
  private final IndexingState indexingState;
  private final BulkProcessor processor;

  // Holding a reference to the client to be able to close it, as the caller might not have reference to it.
  private final Client client;

  /**
   * Status.
   */
  private int documentCount;

  public ElasticSearchDocumentWriter(Client client, String indexName, IndexingState indexingState,
      BulkProcessor bulkProcessor,
      String writerId) {
    this.indexName = indexName;
    this.writerId = writerId;
    this.indexingState = indexingState;
    this.processor = bulkProcessor;
    this.client = client;
    log.info("[{}] Created ES document writer.", writerId);
  }

  @Override
  public void write(Document document) throws IOException {
    byte[] source = createSource(document.getSource());
    write(document.getId(), document.getType(), source);
  }

  protected void write(String id, DocumentType type, byte[] source) {
    if (isBigDocument(source.length)) {
      processor.flush();
    }

    val request = createRequest(id, type, source);
    processor.add(request);
    documentCount++;
  }

  @Override
  public void close() throws IOException {
    // Initiate an index request which will set the pendingBulkRequest
    processor.flush();

    log.info("[{}] Closing bulk processor...", writerId);
    indexingState.waitForPendingRequests();
    processor.close();
    client.close();
    log.info("[{}] Finished indexing {} documents", writerId, formatCount(documentCount));
  }

  protected static byte[] createSource(Object document) {
    try {
      return BINARY_WRITER.writeValueAsBytes(document);
    } catch (JsonProcessingException e) {
      throw propagate(e);
    }
  }

  private IndexRequest createRequest(String id, DocumentType type, byte[] source) {
    return indexRequest(indexName)
        .type(type.getName())
        .id(id)
        .contentType(SMILE)
        .source(source);
  }

  private static boolean isBigDocument(int length) {
    return length > BULK_SIZE.getBytes();
  }

}
