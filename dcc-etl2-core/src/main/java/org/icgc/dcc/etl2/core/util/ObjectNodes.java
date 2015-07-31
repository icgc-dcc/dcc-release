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

import java.util.Set;
import java.util.stream.Collectors;

import lombok.NonNull;
import lombok.val;
import lombok.experimental.UtilityClass;

import org.icgc.dcc.common.core.util.Splitters;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Splitter;

@UtilityClass
public class ObjectNodes {

  public static final ObjectMapper MAPPER = new ObjectMapper();

  private static final Splitter PATH_SPLITTER = Splitters.DOT;

  public static String textValue(JsonNode jsonNode, @NonNull String fieldName) {
    if (jsonNode == null) {
      return null;
    }

    val fieldValue = jsonNode.path(fieldName);

    return fieldValue.isMissingNode() ? null : fieldValue.textValue();
  }

  public static JsonNode getPath(@NonNull ObjectNode objectNode, @NonNull String path) {
    val parts = parsePath(path);

    JsonNode jsonNode = objectNode;
    for (val fieldName : parts) {
      jsonNode = jsonNode.get(fieldName);
      if (jsonNode == null) {
        // Missing
        return null;
      }
    }

    return jsonNode;
  }

  private static Iterable<String> parsePath(String path) {
    val parts = PATH_SPLITTER.split(path);
    return parts;
  }

  public static String toEmptyJsonValue(Set<String> fields) {
    String joined = fields.stream()
        .map(i -> "\"" + i.toString() + "\":\"\"")
        .collect(Collectors.joining(", "));

    return "[" + joined + "]";
  }

}
