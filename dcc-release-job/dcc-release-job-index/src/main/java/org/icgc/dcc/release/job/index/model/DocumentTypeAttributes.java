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
package org.icgc.dcc.release.job.index.model;

import static org.icgc.dcc.release.job.index.model.DocumentFields.DEFAULT_DOCUMENT_FIELDS;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import org.icgc.dcc.common.core.model.Entity;
import org.icgc.dcc.common.core.model.ReleaseCollection;
import org.icgc.dcc.release.core.job.FileType;

@Setter
@Accessors(fluent = true, chain = true)
class DocumentTypeAttributes {

  private final static DocumentClassifier DEFAULT_CLASSIFIER = DocumentClassifier.BASIC;
  private final static int DEFAULT_BATCH_SIZE = 1;
  private final static int DEFAULT_STATUS_INTERVAL = 1000;

  @NonNull
  String name;
  @NonNull
  Entity entity;
  @NonNull
  ReleaseCollection collection;
  @NonNull
  DocumentClassifier classifier = DEFAULT_CLASSIFIER;
  @NonNull
  String indexClassName;
  @NonNull
  FileType outputFileType;
  @NonNull
  DocumentFields fields = DEFAULT_DOCUMENT_FIELDS;
  int batchSize = DEFAULT_BATCH_SIZE;
  int statusInterval = DEFAULT_STATUS_INTERVAL;

  DocumentTypeAttributes fields(@NonNull DocumentFields.Builder builder) {
    this.fields = builder.build();
    return this;
  }

}