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
package org.icgc.dcc.release.job.index.function;

import java.util.Iterator;

import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.icgc.dcc.release.job.index.core.Document;
import org.icgc.dcc.release.job.index.core.DocumentWriter;
import org.icgc.dcc.release.job.index.factory.TransportClientFactory;
import org.icgc.dcc.release.job.index.io.ElasticSearchDocumentWriter;
import org.icgc.dcc.release.job.index.model.DocumentType;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableList;

public final class WriteDocument implements FlatMapFunction<Iterator<Document>, Document> {

  @Getter(lazy = true)
  private final Iterable<DocumentWriter> documentWriters = createDocumentWriters();
  private final DocumentType type;
  private final String esUri;
  private final String indexName;

  public WriteDocument(DocumentType type, String esUri, String indexName) {
    this.type = type;
    this.esUri = esUri;
    this.indexName = indexName;
  }

  private Iterable<DocumentWriter> createDocumentWriters() {
    val client = TransportClientFactory.newTransportClient(esUri);
    return ImmutableList.of(new ElasticSearchDocumentWriter(client, indexName, type, 1));
  }

  @Override
  public Iterable<Document> call(Iterator<Document> partition) throws Exception {
    val result = Lists.<Document> newArrayList();
    while (partition.hasNext()) {
      val element = partition.next();
      write(element);
      result.add(element);
    }
    closeWriters();

    return result;
  }

  @SneakyThrows
  private void closeWriters() {
    for (val writer : getDocumentWriters()) {
      writer.close();
    }
  }

  @SneakyThrows
  private void write(Document document) {
    for (val writer : getDocumentWriters()) {
      writer.write(document);
    }
  }

}