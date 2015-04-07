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
package org.icgc.dcc.etl2.job.export.function;

import static org.icgc.dcc.etl2.core.util.ObjectNodes.textValue;
import lombok.val;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.Function;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;

public class AddDonorIdField implements Function<ObjectNode, ObjectNode> {

  private static final String DONOR_ID = "icgc_donor_id";
  private static final String ICGC_DONOR_ID_PREFIX = "DO";

  private String extractDonorId(ObjectNode row) {
    val donorIdValue = textValue(row, DONOR_ID);
    Preconditions.checkState(donorIdValue.startsWith(ICGC_DONOR_ID_PREFIX));
    val donorId = StringUtils.substringAfter(donorIdValue.trim(), ICGC_DONOR_ID_PREFIX);
    return donorId;
  }

  @Override
  public ObjectNode call(ObjectNode row) {
    return row.put("donor_id", extractDonorId(row));
  }

}