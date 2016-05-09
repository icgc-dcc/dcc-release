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
package org.icgc.dcc.release.core.util;

import static com.google.common.base.Strings.isNullOrEmpty;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.release.core.util.ObjectNodes.textValue;
import lombok.NoArgsConstructor;
import lombok.val;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

// TODO: This class should be removed and replaced with ProjectFields
@NoArgsConstructor(access = PRIVATE)
public final class Keys {

  public static final String KEY_SEPARATOR = "#";
  public static final String NULL_KEY = "NULL_KEY";

  private static final Joiner KEY_JOINER = Joiner.on(KEY_SEPARATOR);

  public static String getKey(ObjectNode row, String... fieldNames) {
    val values = Lists.<String> newArrayListWithCapacity(fieldNames.length);
    for (val fieldName : fieldNames) {
      val value = textValue(row, fieldName);
      // Null keys might come for the 'surgery' and 'biomarker' datatype.
      // FIXME: https://jira.oicr.on.ca/browse/DCC-3956
      if (isNullOrEmpty(value)) {
        values.add(NULL_KEY);
        continue;
      }

      values.add(value);
    }

    return KEY_JOINER.join(values);
  }

  public static String getKey(String... values) {
    return KEY_JOINER.join(values);
  }

}
