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
package org.icgc.dcc.release.job.document.transform;

import lombok.NonNull;
import lombok.val;

import org.apache.spark.api.java.function.Function;
import org.icgc.dcc.release.job.document.context.DefaultDocumentContext;
import org.icgc.dcc.release.job.document.core.Document;
import org.icgc.dcc.release.job.document.core.DocumentContext;
import org.icgc.dcc.release.job.document.core.DocumentTransform;
import org.icgc.dcc.release.job.document.core.DocumentJobContext;
import org.icgc.dcc.release.job.document.model.DocumentClassifier;
import org.icgc.dcc.release.job.document.model.DocumentType;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * {@link DocumentTransform} implementation that simply creates a resulting document consisting of the supplied
 * {@code root}.
 * <p>
 * Intended to be used with {@link DocumentClassifier#BASIC} document types.
 */
public class BasicDocumentTransform implements DocumentTransform, Function<ObjectNode, Document> {

  private final DocumentContext documentContext;

  public BasicDocumentTransform(DocumentType type) {
    this.documentContext = new DefaultDocumentContext(type, DocumentJobContext.builder().build());
  }

  @Override
  public Document call(ObjectNode root) throws Exception {
    return transformDocument(root, documentContext);
  }

  @Override
  public Document transformDocument(@NonNull ObjectNode root, @NonNull DocumentContext context) {
    val type = context.getType();
    val id = getId(root, type);

    return new Document(type, id, root);
  }

  /**
   * Get the id value of the supplied {@code root}.
   *
   * @param root the root object under construction
   * @param type the document type to be created
   * @return
   */
  private static String getId(@NonNull ObjectNode root, @NonNull DocumentType type) {
    val fieldName = type.getCollection().getSurrogateKey();
    val id = root.get(fieldName).asText();

    return id;
  }

}
