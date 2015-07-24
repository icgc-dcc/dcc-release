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
package org.icgc.dcc.etl2.job.index.function;

import static java.util.Collections.emptyList;

import java.net.URI;

import lombok.val;

import org.icgc.dcc.etl2.job.index.core.DocumentContext;
import org.icgc.dcc.etl2.job.index.model.DocumentType;

import scala.Tuple2;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;

public class RowChildrenTransform extends
    AbstractRowTransform<Tuple2<String, Tuple2<ObjectNode, Optional<Iterable<ObjectNode>>>>> {

  public RowChildrenTransform(DocumentType type, String collectionDir, URI fsUri) {
    super(type, collectionDir, fsUri);
  }

  @Override
  public ObjectNode call(Tuple2<String, Tuple2<ObjectNode, Optional<Iterable<ObjectNode>>>> tuple) throws Exception {
    val root = tuple._2._1;
    val children = tuple._2._2.or(emptyList());
    val customDocumentContext = createCustomDocumentContext(children);

    return execute(root, customDocumentContext);
  }

  /**
   * Template method.
   */
  protected DocumentContext createCustomDocumentContext(Iterable<ObjectNode> children) {
    // By default, return the default
    return getDocumentContext();
  }

}