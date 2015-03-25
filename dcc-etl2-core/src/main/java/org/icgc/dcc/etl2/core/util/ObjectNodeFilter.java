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

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

import lombok.NonNull;
import lombok.ToString;
import lombok.Value;
import lombok.val;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
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
  @NonNull
  private final FilterMode mode;
  @NonNull
  private final Map<String, FilterPath> index;

  public ObjectNodeFilter(@NonNull FilterMode mode, String... fieldPaths) {
    this(mode, ImmutableSet.copyOf(fieldPaths));
  }

  public ObjectNodeFilter(@NonNull FilterMode mode, @NonNull Iterable<String> fieldPaths) {
    this.mode = mode;
    this.index = indexFieldPaths(createFilterPaths(fieldPaths));
  }

  public ObjectNode filter(ObjectNode value) {
    filter(value, null);

    return value;
  }

  private void filter(JsonNode value, String parentPath) {
    if (value.isObject()) {
      filterObject(value, parentPath);
    } else if (value.isArray()) {
      filterArray(value, parentPath);
    }
  }

  private void filterObject(JsonNode value, String parentPath) {
    val iterator = value.fields();
    while (iterator.hasNext()) {
      val field = iterator.next();

      val fieldName = field.getKey();
      val fieldValue = field.getValue();
      val fieldPath = qualifyField(parentPath, fieldName);

      if (isRemoveableField(fieldPath)) {
        val filterPath = index.get(fieldPath);
        if (!fieldValue.isContainerNode() || filterPath.isLeaf()) {
          iterator.remove();
          continue;
        }
      }

      filter(fieldValue, fieldPath);
    }
  }

  private void filterArray(JsonNode value, String parentPath) {
    for (val element : value) {
      filter(element, parentPath);
    }
  }

  private static String qualifyField(String parentPath, String fieldName) {
    return parentPath == null ? fieldName : parentPath + FIELD_PATH_SEPARATOR + fieldName;
  }

  private boolean isRemoveableField(String fieldPath) {
    if (mode == FilterMode.INCLUDE) {
      return !index.containsKey(fieldPath);
    } else {
      return index.containsKey(fieldPath);
    }
  }

  private static Set<FilterPath> createFilterPaths(Iterable<String> fieldPaths) {
    val filterPaths = Sets.<FilterPath> newHashSet();

    for (val fieldName : fieldPaths) {
      String[] parts = fieldName.split("\\" + FIELD_PATH_SEPARATOR);

      String path = null;
      for (int i = 0; i < parts.length; i++) {
        val leaf = i == parts.length - 1;
        if (path == null) {
          path = parts[i];
        } else {
          path = qualifyField(path, parts[i]);
        }

        filterPaths.add(new FilterPath(path, leaf));
      }
    }

    return filterPaths;
  }

  private static ImmutableMap<String, FilterPath> indexFieldPaths(Iterable<FilterPath> filterPaths) {
    return Maps.uniqueIndex(filterPaths, new Function<FilterPath, String>() {

      @Override
      public String apply(FilterPath input) {
        return input.getFieldPath();
      }

    });
  }

  public enum FilterMode {
    INCLUDE, EXCLUDE;
  }

  @Value
  public static class FilterPath implements Serializable {

    String fieldPath;
    boolean leaf;

  }

}
