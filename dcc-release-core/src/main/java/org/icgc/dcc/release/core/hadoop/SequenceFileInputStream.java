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
package org.icgc.dcc.release.core.hadoop;

import java.io.IOException;
import java.io.InputStream;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.icgc.dcc.release.core.util.JacksonFactory;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class SequenceFileInputStream extends InputStream {

  private final SequenceFile.Reader reader;
  private Buffer buffer;

  public SequenceFileInputStream(@NonNull Configuration configuration, @NonNull Path path) throws IOException {
    reader = new SequenceFile.Reader(configuration, Reader.file(path));
  }

  @Override
  public int read() throws IOException {
    if (buffer == null || !buffer.hasNext()) {
      if (!readBytes()) {
        return -1;
      }
    }

    return buffer.next() & 0xFF;
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  @SneakyThrows
  private boolean readBytes() {
    val writable = new BytesWritable();
    val read = reader.next(NullWritable.get(), writable);
    if (!read) {
      return false;
    }

    buffer = new Buffer(getBytes(writable));

    return true;
  }

  private static byte[] getBytes(BytesWritable bw) {
    byte[] padded = bw.getBytes();
    byte[] bytes = new byte[bw.getLength()];
    System.arraycopy(padded, 0, bytes, 0, bytes.length);

    return bytes;
  }

  private static class Buffer {

    private final byte[] data;
    private int position = 0;

    @SneakyThrows
    public Buffer(byte[] smileEncodedBytes) {
      val json = JacksonFactory.READER.<ObjectNode> readValue(smileEncodedBytes);
      data = JacksonFactory.MAPPER.writeValueAsBytes(json);
    }

    public boolean hasNext() {
      return data.length > position;
    }

    public byte next() {
      return data[position++];
    }

  }

}
