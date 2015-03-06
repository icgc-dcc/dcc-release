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
package org.icgc.dcc.etl2.core.function;

import static org.icgc.dcc.common.core.model.SpecialValue.NO_VALUE;
import static org.icgc.dcc.common.core.model.SpecialValue.isDeprecatedValue;
import static org.icgc.dcc.common.core.model.SpecialValue.isFullMissingCode;
import lombok.val;

import org.apache.spark.api.java.function.Function;

import com.fasterxml.jackson.databind.node.ObjectNode;

// TODO: Move to stage module
public class TranslateMissingCode implements Function<ObjectNode, ObjectNode> {

  @Override
  public ObjectNode call(ObjectNode row) throws Exception {
    val iterator = row.fieldNames();
    while (iterator.hasNext()) {
      val fieldName = iterator.next();
      val value = row.get(fieldName).textValue();
      if (isMissingCode(value)) {
        // Replace any empty value, missing code or former missing code with null
        val replaced = (String) NO_VALUE;

        row.put(fieldName, replaced);
      }
    }

    return row;
  }

  private static boolean isMissingCode(String value) {
    return value.isEmpty() || isFullMissingCode(value) || isDeprecatedValue(value);
  }

}