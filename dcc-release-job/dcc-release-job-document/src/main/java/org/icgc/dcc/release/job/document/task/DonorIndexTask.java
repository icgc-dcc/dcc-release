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
package org.icgc.dcc.release.job.document.task;

import lombok.val;

import org.apache.spark.api.java.JavaRDD;
import org.icgc.dcc.release.core.document.DocumentType;
import org.icgc.dcc.release.core.document.Document;
import org.icgc.dcc.release.core.task.TaskContext;
import org.icgc.dcc.release.job.document.core.DocumentJobContext;
import org.icgc.dcc.release.job.document.transform.DonorDocumentTransform;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class DonorIndexTask extends AbstractIndexTask {

  private final DocumentJobContext indexJobContext;

  public DonorIndexTask(DocumentJobContext indexJobContext) {
    super(DocumentType.DONOR_TYPE);
    this.indexJobContext = indexJobContext;
  }

  @Override
  public void execute(TaskContext taskContext) {
    val donors = readDonors(taskContext);
    val output = transform(donors);

    writeDocOutput(taskContext, output);
  }

  private JavaRDD<Document> transform(JavaRDD<ObjectNode> donors) {
    return donors.map(new DonorDocumentTransform(indexJobContext));
  }

}
