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
package org.icgc.dcc.etl2.core.hadoop;

import static org.icgc.dcc.etl2.core.hadoop.ObjectNodeSerialization.READER;
import static org.icgc.dcc.etl2.core.hadoop.ObjectNodeSerialization.WRITER;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.val;

import org.apache.hadoop.io.Writable;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.node.ObjectNode;

@NoArgsConstructor
@AllArgsConstructor
public class ObjectNodeWritable implements Writable {

  private ObjectNode value;

  public ObjectNode get() {
    return this.value;
  }

  public void set(ObjectNode value) {
    this.value = value;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    val stream = new DataOutputOutputStream(out);

    writeValue(stream);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    val stream = new DataInputInputStream(in);

    this.value = readValue(stream);

  }

  private void writeValue(OutputStream stream) throws IOException, JsonGenerationException, JsonMappingException {
    WRITER.writeValue(stream, value);
  }

  private ObjectNode readValue(InputStream stream) throws IOException, JsonProcessingException {
    return READER.readValue(stream);
  }

  private static class DataOutputOutputStream extends OutputStream {

    private final DataOutput out;

    private DataOutputOutputStream(DataOutput out) {
      this.out = out;
    }

    @Override
    public void write(int b) throws IOException {
      out.writeByte(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      out.write(b, off, len);
    }

    @Override
    public void write(byte[] b) throws IOException {
      out.write(b);
    }

  }

  private static class DataInputInputStream extends InputStream {

    private DataInput in;

    public DataInputInputStream(DataInput in) {
      this.in = in;
    }

    @Override
    public int read() throws IOException {
      return in.readUnsignedByte();
    }

  }

}
