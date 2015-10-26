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

import static com.google.common.base.Preconditions.checkState;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.smile.SmileGenerator;
import com.fasterxml.jackson.dataformat.smile.SmileParser;

import lombok.SneakyThrows;
import lombok.val;

public final class SmileSerializer extends Serializer<ObjectNode> {

  /**
   * Constants.
   */
  private static final JsonFactory FACTORY = new SmileFactory()
      .disable(SmileGenerator.Feature.WRITE_HEADER)
      .disable(SmileParser.Feature.REQUIRE_HEADER)
      .disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET)
      .disable(JsonParser.Feature.AUTO_CLOSE_SOURCE);

  private static final ObjectMapper MAPPER = new ObjectMapper(FACTORY)
      .disable(SerializationFeature.CLOSE_CLOSEABLE);

  private static final ObjectWriter WRITER = MAPPER.writerWithType(ObjectNode.class);
  private static final ObjectReader READER = MAPPER.reader(ObjectNode.class);

  private static final int NULL_VALUE = 0;

  @Override
  @SneakyThrows
  public void write(Kryo kryo, Output output, ObjectNode object) {
    if (object == null) {
      output.writeInt(NULL_VALUE, true);
      output.flush();

      return;
    }

    byte[] bytes = WRITER.writeValueAsBytes(object);
    output.writeInt(bytes.length, true);
    output.write(bytes);
    output.flush();
  }

  @Override
  @SneakyThrows
  public ObjectNode read(Kryo kryo, Input input, Class<ObjectNode> type) {
    val payloadLength = input.readInt(true);
    if (payloadLength == NULL_VALUE) {

      return null;
    }

    val capacity = getCapacity(input);
    val canReuseInputBuffer = payloadLength <= capacity;
    JsonNode jsonNode = null;
    if (canReuseInputBuffer) {
      jsonNode = readJsonNode(input, payloadLength);
    } else {
      jsonNode = READER.readValue(input.readBytes(payloadLength));
    }

    checkState(jsonNode.isObject(), "Failed to convert %s to ObjectNode", jsonNode);

    return (ObjectNode) jsonNode;
  }

  @SneakyThrows
  private static JsonNode readJsonNode(Input input, int payloadLength) {
    val position = input.position();
    JsonNode jsonNode = READER.readValue(input.getBuffer(), position, payloadLength);
    input.setPosition(position + payloadLength);

    return jsonNode;
  }

  private static int getCapacity(Input input) {
    byte[] buffer = input.getBuffer();

    return buffer.length - input.position();
  }

}