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
package org.icgc.dcc.release.job.summarize.function;

import static com.google.common.base.Strings.isNullOrEmpty;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_GENE_GENE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_ID;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_TYPE;
import static org.icgc.dcc.release.core.util.FieldNames.SummarizeFieldNames.FAKE_GENE_ID;
import lombok.val;

import org.icgc.dcc.release.core.function.RetainFields;

import com.fasterxml.jackson.databind.node.ObjectNode;

public final class RetainGeneFields extends RetainFields {

  // Including _mutation_id to filter unique geneIds in one donor
  public RetainGeneFields() {
    super(OBSERVATION_TYPE, MUTATION_ID, DONOR_GENE_GENE_ID);
  }

  @Override
  public ObjectNode call(ObjectNode row) {
    val result = super.call(row);
    val geneId = result.path(DONOR_GENE_GENE_ID);
    if (geneId.isMissingNode() || isNullOrEmpty(geneId.textValue())) {
      result.put(DONOR_GENE_GENE_ID, FAKE_GENE_ID);
    }

    return result;
  }

}