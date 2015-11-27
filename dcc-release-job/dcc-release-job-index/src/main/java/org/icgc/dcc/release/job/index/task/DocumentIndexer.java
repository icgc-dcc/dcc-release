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
package org.icgc.dcc.release.job.index.task;

import java.util.Iterator;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.icgc.dcc.release.core.document.BaseDocumentType;
import org.icgc.dcc.release.core.document.Document;
import org.icgc.dcc.release.core.document.DocumentWriter;
import org.icgc.dcc.release.job.index.factory.TransportClientFactory;
import org.icgc.dcc.release.job.index.io.ElasticSearchDocumentWriter;

import com.google.common.collect.Lists;

@RequiredArgsConstructor
public final class DocumentIndexer implements FlatMapFunction<Iterator<Document>, Void> {

  @NonNull
  private final String esUri;
  @NonNull
  private final String indexName;
  @NonNull
  private final BaseDocumentType documentType;

  private transient DocumentWriter documentWriter;

  @Override
  public Iterable<Void> call(Iterator<Document> t) throws Exception {
    checkDocumentWriter();

    while (t.hasNext()) {
      documentWriter.write(t.next());
    }

    documentWriter.close();
    documentWriter = null;

    return Lists.newArrayList();
  }

  private void checkDocumentWriter() {
    if (documentWriter == null) {
      val client = TransportClientFactory.newTransportClient(esUri, true);
      documentWriter = new ElasticSearchDocumentWriter(client, indexName, documentType, 1);
    }
  }

}