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

import static lombok.AccessLevel.PRIVATE;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.val;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.smile.SmileGenerator;
import com.fasterxml.jackson.dataformat.smile.SmileParser;

@NoArgsConstructor(access = PRIVATE)
public final class JacksonFactory {

  public static final Class<ObjectNode> DEFAULT_CLASS = ObjectNode.class;

  public static final JsonFactory DEFAULT_SMILE_FACOTRY = new SmileFactory();

  public static final JsonFactory SMILE_FACTORY = new SmileFactory()
      .disable(SmileGenerator.Feature.WRITE_HEADER)
      .disable(SmileParser.Feature.REQUIRE_HEADER)

      .disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET)
      .disable(JsonParser.Feature.AUTO_CLOSE_SOURCE);

  public static final ObjectMapper MAPPER = createMapper(null);

  public static final ObjectMapper DEFAULT_SMILE_MAPPER = new ObjectMapper(DEFAULT_SMILE_FACOTRY);
  public static final ObjectMapper SMILE_MAPPER = new ObjectMapper(SMILE_FACTORY)
      .disable(SerializationFeature.CLOSE_CLOSEABLE);

  public static final ObjectWriter WRITER = MAPPER.writerWithType(ObjectNode.class);
  public static final ObjectReader READER = MAPPER.reader(ObjectNode.class);

  public static final ObjectWriter SMILE_WRITER = SMILE_MAPPER.writerWithType(ObjectNode.class);
  public static final ObjectWriter DEFAULT_SMILE_WRITER = DEFAULT_SMILE_MAPPER.writerWithType(ObjectNode.class);
  public static final ObjectReader SMILE_READER = SMILE_MAPPER.reader(ObjectNode.class);

  public static <T> ObjectReader createSmileObjectReader(@NonNull Class<T> clazz) {
    return ObjectNode.class.equals(clazz) ? SMILE_READER : SMILE_MAPPER.reader(clazz);
  }

  public static <T> ObjectWriter createSmileObjectWriter(@NonNull Class<T> clazz) {
    return ObjectNode.class.equals(clazz) ? SMILE_WRITER : SMILE_MAPPER.writerWithType(clazz);
  }

  public static <T> ObjectReader createObjectReader(@NonNull Class<T> clazz) {
    return ObjectNode.class.equals(clazz) ? READER : MAPPER.reader(clazz);
  }

  public static <T> ObjectWriter createObjectWriter(@NonNull Class<T> clazz) {
    return ObjectNode.class.equals(clazz) ? WRITER : MAPPER.writerWithType(clazz);
  }

  private static ObjectMapper createMapper(JsonFactory factory) {
    val mapper = factory == null ? new ObjectMapper() : new ObjectMapper(factory);
    mapper.setSerializationInclusion(Include.NON_NULL);

    return mapper;
  }

}
