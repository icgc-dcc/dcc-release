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
package org.icgc.dcc.etl2.core.util;

import static com.google.common.base.Preconditions.checkState;

import java.io.Serializable;
import java.util.Set;

import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.val;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

@ToString
public class ObjectNodeFilter implements Serializable {

  /**
   * Constants.
   */
  private static final String FIELD_PATH_SEPARATOR = ".";

  /**
   * Configuration.
   */
  @Getter
  @NonNull
  private final FilterMode mode;
  @NonNull
  private final Set<String> filterPaths;
  @NonNull
  private final Set<String> inferredFilterPaths;

  public ObjectNodeFilter(@NonNull FilterMode mode, String... filterPaths) {
    this(mode, ImmutableSet.copyOf(filterPaths));
  }

  public ObjectNodeFilter(@NonNull FilterMode mode, @NonNull Set<String> filterPaths) {
    this.mode = mode;
    this.filterPaths = filterPaths;
    this.inferredFilterPaths = mode == FilterMode.INCLUDE ? inferFilterPaths(filterPaths) : filterPaths;
  }

  public ObjectNode filter(ObjectNode value) {
    filter(value, null);

    return value;
  }

  private void filter(JsonNode value, String path) {
    if (value.isObject()) {
      filterObject(value, path);
    } else if (value.isArray()) {
      filterArray(value, path);
    } else {
      checkState(!value.isContainerNode());
    }
  }

  private void filterObject(JsonNode value, String path) {
    if (isSkipped(path)) {
      return;
    }

    val iterator = value.fields();
    while (iterator.hasNext()) {
      val field = iterator.next();
      val fieldName = field.getKey();
      val fieldValue = field.getValue();
      val fieldPath = qualifyField(path, fieldName);

      if (isRemovable(fieldPath)) {
        iterator.remove();
      } else {
        filter(fieldValue, fieldPath);
      }
    }
  }

  private boolean isSkipped(String path) {
    return filterPaths.contains(path) && mode == FilterMode.INCLUDE;
  }

  private boolean isRemovable(String fieldPath) {
    val present = filterPaths.contains(fieldPath);

    return mode == FilterMode.INCLUDE && !present || mode == FilterMode.EXCLUDE && present;
  }

  private void filterArray(JsonNode value, String path) {
    for (val element : value) {
      filter(element, path);
    }
  }

  private static Set<String> inferFilterPaths(Iterable<String> filterPaths) {
    val inferredPaths = Sets.<String> newHashSet();
  
    for (val fieldName : filterPaths) {
      String[] parts = fieldName.split("\\" + FIELD_PATH_SEPARATOR);
  
      String path = null;
      for (val part : parts) {
        if (path == null) {
          path = part;
        } else {
          path = qualifyField(path, part);
        }
  
        inferredPaths.add(path);
      }
    }
  
    return inferredPaths;
  }

  private static String qualifyField(String path, String fieldName) {
    return path == null ? fieldName : path + FIELD_PATH_SEPARATOR + fieldName;
  }

  public enum FilterMode {
    INCLUDE, EXCLUDE;
  }

}
