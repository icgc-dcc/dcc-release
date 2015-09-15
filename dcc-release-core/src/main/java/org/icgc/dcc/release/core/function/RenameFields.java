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
package org.icgc.dcc.release.core.function;

import java.util.Map;

import lombok.NonNull;
import lombok.val;

import org.apache.spark.api.java.function.Function;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Maps;

public class RenameFields implements Function<ObjectNode, ObjectNode> {

  @NonNull
  private final Map<String, String> mappings;

  public RenameFields(Map<String, String> values) {
    this.mappings = values;
  }

  public RenameFields(String... values) {
    val mappings = Maps.<String, String> newHashMap();
    for (int i = 0; i < values.length; i += 2) {
      val fieldName = values[i];
      val path = values[i + 1];
      mappings.put(fieldName, path);
    }
    this.mappings = mappings;
  }

  @Override
  public ObjectNode call(ObjectNode row) {
    for (val entry : mappings.entrySet()) {
      val oldFieldName = entry.getKey();
      val newFieldName = entry.getValue();
      val value = row.get(oldFieldName);
      row.remove(oldFieldName);
      row.put(newFieldName, value);
    }

    return row;
  }

}