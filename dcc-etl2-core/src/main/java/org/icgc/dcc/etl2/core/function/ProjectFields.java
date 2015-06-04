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

import java.util.Map;

import lombok.NonNull;
import lombok.val;

import org.apache.spark.api.java.function.Function;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Maps;

public class ProjectFields implements Function<ObjectNode, ObjectNode> {

  @NonNull
  private final Map<String, String> projections;

  public ProjectFields(Map<String, String> values) {
    this.projections = values;
  }

  public ProjectFields(String... values) {
    val projections = Maps.<String, String> newHashMap();
    for (int i = 0; i < values.length; i += 2) {
      val fieldName = values[i];
      val path = values[i + 1];
      projections.put(fieldName, path);
    }

    this.projections = projections;
  }

  @Override
  public ObjectNode call(ObjectNode row) {
    val newRow = row.objectNode();
    val fields = row.fields();
    while (fields.hasNext()) {
      val entry = fields.next();
      val fieldName = entry.getKey();
      val value = entry.getValue();
      if (projections.containsKey(fieldName)) {
        newRow.put(projections.get(fieldName), value);
      }
    }

    return newRow;
  }

}
