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
package org.icgc.dcc.release.job.stage.function;

import java.util.Map;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.spark.api.java.function.Function;
import org.icgc.dcc.release.core.submission.SubmissionFileSchema;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Maps;

@RequiredArgsConstructor
public class TranslateCodeListTerm implements Function<ObjectNode, ObjectNode> {

  @NonNull
  private final Map<String, Map<String, String>> fieldTerms = Maps.newHashMap();

  public TranslateCodeListTerm(@NonNull SubmissionFileSchema schema) {
    for (val field : schema.getFields()) {
      if (field.getTerms() != null) {
        fieldTerms.put(field.getName(), field.getTerms());
      }
    }
  }

  @Override
  public ObjectNode call(ObjectNode row) throws Exception {
    for (val entry : fieldTerms.entrySet()) {
      val fieldName = entry.getKey();
      val terms = entry.getValue();

      val originalValue = row.get(fieldName).textValue();
      // Translate term code to value
      if (originalValue != null && terms.containsKey(originalValue)) {
        val translated = terms.get(originalValue);
        row.put(fieldName, translated);
      }
    }

    return row;
  }

}