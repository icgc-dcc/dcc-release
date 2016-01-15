/*
 * Copyright (c) 2013 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.release.test.util;

import static com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_COMMENTS;
import static com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_SINGLE_QUOTES;
import static com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES;
import static com.google.common.collect.ImmutableList.copyOf;

import java.io.File;
import java.util.List;

import lombok.SneakyThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class TestJsonNodes {

  /**
   * Allow for more liberal JSON strings to simplify literals with constants, etc.
   */
  public static final ObjectMapper MAPPER = new ObjectMapper() //
      .configure(ALLOW_UNQUOTED_FIELD_NAMES, true) //
      .configure(ALLOW_SINGLE_QUOTES, true) //
      .configure(ALLOW_COMMENTS, true);

  /**
   * Utility method that returns a {@code JsonNode} given a JSON String.
   * <p>
   * The name and use is inspired by jQuery's {@code $} function.
   * 
   * @param json
   * @return
   */
  @SneakyThrows
  public static ObjectNode $(String json) {
    return (ObjectNode) MAPPER.readTree(json);
  }

  /**
   * Utility method that returns a {@code JsonNode} given a JSON String.
   * <p>
   * The name and use is inspired by jQuery's {@code $} function.
   * 
   * @param json
   * @return
   */
  @SneakyThrows
  public static ObjectNode $(File jsonFile) {
    return (ObjectNode) MAPPER.readTree(jsonFile);
  }

  @SneakyThrows
  public static String toJson(Object object) {
    return MAPPER.writeValueAsString(object);
  }

  public static List<JsonNode> getElements(JsonNode node) {
    return copyOf(node.elements());
  }

}
