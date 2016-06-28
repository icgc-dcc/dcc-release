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

import static org.icgc.dcc.release.job.index.io.DocumentWriterFactory.createDocumentWriter;
import static org.mockito.Mockito.when;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.transport.ReceiveTimeoutTransportException;
import org.icgc.dcc.common.core.json.Jackson;
import org.icgc.dcc.release.core.document.Document;
import org.icgc.dcc.release.core.document.DocumentType;
import org.icgc.dcc.release.core.document.DocumentWriter;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class ElasticSearchDocumentWriterTest {

  private static final String INDEX = "elasticsearch-document-writer-test";

  @Mock
  Client esClient;
  @Mock
  ActionFuture<BulkResponse> action;
  @Mock
  DiscoveryNode node;
  @Mock
  ReceiveTimeoutTransportException exception;

  DocumentWriter writer;

  @Before
  public void setUp() {
    writer = createDocumentWriter(esClient, INDEX, DocumentType.RELEASE_TYPE);
  }

  @Test
  @SneakyThrows
  @Ignore("Ignored as can't reproduce failure with this test so far.")
  public void processTest() {
    when(esClient.bulk(Matchers.any(BulkRequest.class))).thenThrow(exception);

    writer.write(createDocument());
    log.info("Close");
    writer.close();
    log.info("Exit.");
  }

  private Document createDocument() {
    return new Document(DocumentType.RELEASE_TYPE, "ICGC", Jackson.DEFAULT.createObjectNode());
  }

}
