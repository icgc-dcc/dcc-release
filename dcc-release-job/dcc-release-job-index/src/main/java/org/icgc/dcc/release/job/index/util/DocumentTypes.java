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
package org.icgc.dcc.release.job.index.util;

import static com.google.common.base.Preconditions.checkState;
import static lombok.AccessLevel.PRIVATE;

import java.util.Map;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.val;
import lombok.experimental.Accessors;

import org.icgc.dcc.release.core.document.BaseDocumentType;

import com.google.common.collect.ImmutableMap;

@NoArgsConstructor(access = PRIVATE)
public final class DocumentTypes {

  private static final Map<BaseDocumentType, DocumentTypeAttributes> DOCUMENT_TYPES = defineDocumentTypes();

  public static int getBatchSize(@NonNull BaseDocumentType documentType) {
    return DOCUMENT_TYPES.get(documentType).batchSize;
  }

  public static int getStatusInterval(@NonNull BaseDocumentType documentType) {
    return DOCUMENT_TYPES.get(documentType).statusInterval;
  }

  private static Map<BaseDocumentType, DocumentTypeAttributes> defineDocumentTypes() {
    val documentTypes = ImmutableMap.<BaseDocumentType, DocumentTypeAttributes> builder()
        .put(BaseDocumentType.DIAGRAM_TYPE, attributes().batchSize(100).statusInterval(100))
        .put(BaseDocumentType.RELEASE_TYPE, attributes().batchSize(1000).statusInterval(1))
        .put(BaseDocumentType.GENE_SET_TYPE, attributes().statusInterval(1000).batchSize(1000))
        .put(BaseDocumentType.GENE_SET_TEXT_TYPE, attributes().statusInterval(1000).batchSize(1000))
        .put(BaseDocumentType.PROJECT_TYPE, attributes().batchSize(100).statusInterval(10))
        .put(BaseDocumentType.PROJECT_TEXT_TYPE, attributes().batchSize(100).statusInterval(10))
        .put(BaseDocumentType.DONOR_TYPE, attributes().batchSize(10000).statusInterval(1000))
        .put(BaseDocumentType.DONOR_TEXT_TYPE, attributes().batchSize(10000).statusInterval(1000))
        .put(BaseDocumentType.DONOR_CENTRIC_TYPE, attributes().statusInterval(1000).batchSize(1))
        .put(BaseDocumentType.GENE_TYPE, attributes().batchSize(1000).statusInterval(1000))
        .put(BaseDocumentType.GENE_TEXT_TYPE, attributes().batchSize(1000).statusInterval(1000))
        .put(BaseDocumentType.GENE_CENTRIC_TYPE, attributes().batchSize(1000).statusInterval(1000))
        .put(BaseDocumentType.OBSERVATION_CENTRIC_TYPE, attributes().batchSize(200).statusInterval(100000))
        .build();

    checkState(documentTypes.size() == BaseDocumentType.values().length);

    return documentTypes;
  }

  private static DocumentTypeAttributes attributes() {
    return new DocumentTypeAttributes();
  }

  @Setter
  @Accessors(fluent = true, chain = true)
  private static class DocumentTypeAttributes {

    private final static int DEFAULT_BATCH_SIZE = 1;
    private final static int DEFAULT_STATUS_INTERVAL = 1000;

    int batchSize = DEFAULT_BATCH_SIZE;
    int statusInterval = DEFAULT_STATUS_INTERVAL;

  }

}
