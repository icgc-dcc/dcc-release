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

import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getDonorId;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getDonorProjectId;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.setDonorProject;
import lombok.NonNull;
import lombok.val;

import org.apache.spark.api.java.function.Function;
import org.icgc.dcc.release.core.document.BaseDocumentType;
import org.icgc.dcc.release.core.document.Document;
import org.icgc.dcc.release.job.document.context.DefaultDocumentContext;
import org.icgc.dcc.release.job.document.core.DocumentContext;
import org.icgc.dcc.release.job.document.core.DocumentJobContext;
import org.icgc.dcc.release.job.document.core.DocumentTransform;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * {@link DocumentTransform} implementation that creates a donor document.
 */
public class DonorDocumentTransform implements DocumentTransform, Function<ObjectNode, Document> {

  private DocumentContext documentContext;

  public DonorDocumentTransform(DocumentJobContext indexJobContext) {
    documentContext = new DefaultDocumentContext(BaseDocumentType.DONOR_TYPE, indexJobContext);
  }

  @Override
  public Document call(ObjectNode donor) throws Exception {
    return transformDocument(donor, documentContext);
  }

  @Override
  public Document transformDocument(@NonNull ObjectNode donor, @NonNull DocumentContext context) {
    // Identifiers
    val donorId = getDonorId(donor);
    val donorProjectId = getDonorProjectId(donor);

    // Embed
    val donorProject = context.getProject(donorProjectId);
    setDonorProject(donor, donorProject);

    return new Document(context.getType(), donorId, donor);
  }

}
