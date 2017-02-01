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
import static org.icgc.dcc.dcc.common.es.impl.DocumentWriterContextFactory.createContext;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.dcc.common.es.core.DocumentWriter;

@NoArgsConstructor(access = PRIVATE)
public final class DocumentWriterFactory {

  public static DocumentWriter createDocumentWriter(@NonNull String esUri, @NonNull String indexName) {
    return org.icgc.dcc.dcc.common.es.DocumentWriterFactory.createDocumentWriter(indexName, esUri);
  }

  public static DocumentWriter createDocumentWriter(@NonNull DocumentWriterContext context) {
    return org.icgc.dcc.dcc.common.es.DocumentWriterFactory.createDocumentWriter(
        context.getIndexName(), context.getEsUri());
  }

  public static DocumentWriter createFilteringDocumentWriter(@NonNull DocumentWriterContext context) {
    val writerContext = createContext(context.getIndexName(), context.getEsUri());

    return new FilteringElasticSearchDocumentWriter(writerContext, context.getDocumentThreshold(),
        context.getWorkingDir(),
        context.getFsSettings());
  }

}
