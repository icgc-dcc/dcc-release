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
package org.icgc.dcc.etl2.job.index.core;

import static com.google.common.base.Throwables.propagate;
import static com.google.common.collect.Lists.newArrayList;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.common.core.util.FormatUtils.formatRate;
import static org.icgc.dcc.etl2.job.index.model.DocumentType.GENE_TYPE;

import java.util.List;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl2.core.util.Stopwatches;
import org.icgc.dcc.etl2.job.index.model.DocumentType;
import org.icgc.dcc.etl2.job.index.util.DefaultDocumentContext;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Stopwatch;

/**
 * Controls the processing of single {link DocumentType}.
 */
@Slf4j
@RequiredArgsConstructor
public class DocumentProcessor {

  /**
   * Metadata.
   */
  @NonNull
  protected final String indexName;
  @NonNull
  protected final DocumentType type;

  /**
   * Data access.
   */
  @NonNull
  protected final CollectionReader reader;

  /**
   * Callback clients.
   */
  protected final List<DocumentCallback> callbacks = newArrayList();

  /**
   * Add a document life cycle callback.
   * 
   * @param callback
   */
  public void addCallback(@NonNull DocumentCallback callback) {
    callbacks.add(callback);
  }

  /**
   * Process all root and their resulting documents.
   */
  public void process() {
    // Loop state
    int documentCount = 0;
    Stopwatch watch = Stopwatches.createStarted();

    // Document-shared context
    val context = createContext();

    // Process each collection root object in turn
    for (val root : readCollection()) {
      transform(type.getTransform(), context, root);

      boolean status = ++documentCount % type.getStatusInterval() == 0;
      if (status) {
        log.info("Processed {} '{}' documents ({} docs/s) in {}",
            new Object[] { formatCount(documentCount), type.getName(), formatRate(documentCount, watch), watch });
      }
    }

    log.info("Finished processing {} '{}' documents in {}",
        new Object[] { formatCount(documentCount), type.getName(), watch });
  }

  private void transform(DocumentTransform transform, DocumentContext context, ObjectNode root) {
    try {
      // Delegate to transform
      val document = transform.transformDocument(root, context);

      // Delegate behavior to callbacks
      for (val callback : callbacks) {
        callback.call(document);
      }
    } catch (Exception e) {
      log.error("Error procesing document with root: " + root, e);
      propagate(e);
    }
  }

  private DocumentContext createContext() {
    return new DefaultDocumentContext(indexName, type, reader);
  }

  protected Iterable<ObjectNode> readCollection() {
    // Shorthands
    val collection = type.getCollection();
    val fields = type.getFields();
    if (type == GENE_TYPE) {
      // NOTE: Special case
      return reader.readGenes(fields.getFields(collection));
    } else {
      return reader.read(collection, fields.getFields(collection));
    }
  }

}
