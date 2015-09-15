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

import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.common.core.util.FormatUtils.formatRate;
import static org.icgc.dcc.release.core.util.Stopwatches.createStarted;

import java.io.IOException;
import java.net.URI;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.Function;
import org.icgc.dcc.release.job.index.core.CollectionReader;
import org.icgc.dcc.release.job.index.core.Document;
import org.icgc.dcc.release.job.index.core.DocumentContext;
import org.icgc.dcc.release.job.index.io.HDFSCollectionReader;
import org.icgc.dcc.release.job.index.model.DocumentType;
import org.icgc.dcc.release.job.index.util.DefaultDocumentContext;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Stopwatch;

@Slf4j
@RequiredArgsConstructor
public abstract class AbstractRowTransform<F> implements Function<F, ObjectNode> {

  /**
   * Configuration.
   */
  private final DocumentType type;
  private final String collectionDir;
  private final URI fsUri;

  /**
   * State.
   */
  private DocumentContext documentContext;
  private int documentCount;
  private final transient Stopwatch watch = createStarted();

  protected ObjectNode execute(ObjectNode root, DocumentContext documentContext) {
    val document = transformDocument(root, documentContext);

    boolean status = ++documentCount % type.getStatusInterval() == 0;
    if (status) {
      log.info("Processed {} '{}' documents ({} docs/s) in {}",
          formatCount(documentCount), type.getName(), formatRate(documentCount, watch), watch);
    }

    return document.getSource();
  }

  @SneakyThrows
  protected DocumentContext getDocumentContext() {
    if (documentContext == null) {
      this.documentContext = new DefaultDocumentContext(type, createCollectionReader());
    }

    return documentContext;
  }

  private Document transformDocument(ObjectNode root, DocumentContext context) {
    val transform = type.getTransform();

    return transform.transformDocument(root, context);
  }

  private CollectionReader createCollectionReader() throws IOException {
    val fileSystem = FileSystem.get(fsUri, new Configuration());

    return new HDFSCollectionReader(new Path(collectionDir), fileSystem);
  }

}