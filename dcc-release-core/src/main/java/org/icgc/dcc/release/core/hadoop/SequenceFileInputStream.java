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

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

import java.io.IOException;
import java.io.InputStream;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;

public class SequenceFileInputStream extends InputStream {

  private final SequenceFile.Reader reader;
  private Buffer buffer;

  public SequenceFileInputStream(@NonNull Configuration configuration, @NonNull Path path) throws IOException {
    reader = new SequenceFile.Reader(configuration, Reader.file(path));
  }

  @Override
  public int read() throws IOException {
    if (buffer == null || !buffer.hasNext()) {
      if (!readMore()) {
        return -1;
      }
    }

    int b = buffer.next() & 0x000000ff;
    checkState(b >= 0 && b <= 255, format("%d", b));

    return b;
  }

  @SneakyThrows
  private boolean readMore() {
    val writable = new BytesWritable();
    val read = reader.next(NullWritable.get(), writable);
    if (!read) {
      return false;
    }

    byte[] bytes = getBytes(writable);
    buffer = new Buffer(bytes);

    return true;
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  private static byte[] getBytes(BytesWritable bw) {
    // byte[] padded = bw.getBytes();
    // byte[] bytes = new byte[bw.getLength()];
    // System.arraycopy(padded, 0, bytes, 0, bytes.length);
    //
    // return bytes;
    return bw.copyBytes();
  }

  @RequiredArgsConstructor
  private static class Buffer {

    @NonNull
    private final byte[] data;
    private int position = 0;

    public boolean hasNext() {
      return data.length > position;
    }

    public byte next() {
      return data[position++];
    }

  }

}
