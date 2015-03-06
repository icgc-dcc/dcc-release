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
package org.icgc.dcc.etl2.job.imports.hadoop;

import static lombok.AccessLevel.PRIVATE;

import java.io.IOException;

import lombok.NoArgsConstructor;
import lombok.val;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.mongodb.hadoop.io.BSONWritable;

@NoArgsConstructor(access = PRIVATE)
public final class MongoWritables {

  /**
   * Constants.
   */
  private static final SimpleModule BSON_MODULE = new SimpleModule().addSerializer(ObjectId.class,
      new ObjectIdSerializer());
  private static final ObjectMapper BSON_MAPPER = new ObjectMapper().registerModule(BSON_MODULE);

  public static ObjectNode convertBSONWritable(BSONWritable bsonWritable) {
    if (bsonWritable == null) {
      return null;
    }

    val bsonObject = convert(bsonWritable);
    return BSON_MAPPER.convertValue(bsonObject, ObjectNode.class);
  }

  private static BSONObject convert(BSONWritable bsonWritable) {
    val bsonObject = new BasicBSONObject();
    bsonObject.putAll(bsonWritable);

    return bsonObject;
  }

  /**
   * Required to {@code String}ize {@code ObjectId}.
   */
  public static class ObjectIdSerializer extends StdSerializer<ObjectId> {

    public ObjectIdSerializer() {
      super(ObjectId.class);
    }

    @Override
    public void serialize(ObjectId value, JsonGenerator jsonGenerator, SerializerProvider provider) throws IOException {
      // Stringize
      jsonGenerator.writeString(value.toString());
    }

  }

}
