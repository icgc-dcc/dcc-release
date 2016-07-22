/*
 * Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.                             
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

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkArgument;
import static org.icgc.dcc.common.core.util.Separators.TAB;
import static org.icgc.dcc.release.core.hadoop.SmileSequenceFileInputStream.getBytes;
import static org.icgc.dcc.release.core.util.JacksonFactory.SMILE_READER;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;

import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.val;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.icgc.dcc.release.core.hadoop.SmileSequenceFileInputStream;

public class SmileSequenceFileDecompressor {

  public static void main(String... args) {
    checkArgument(args.length == 2, "Expected input and output file");
    val inFile = new File(args[0]);
    val outFile = new File(args[1]);
    checkArgument(inFile.canRead(), "Can't read file %s", inFile);

    execute(inFile, outFile);
  }

  @SneakyThrows
  public static void execute(File inFile, File outFile) {
    @Cleanup
    val in = getInputStream(inFile);
    @Cleanup
    val out = getOutWriter(outFile);

    val reader = getReader(inFile);
    if (hasKey(reader)) {
      decompressWithKey(reader, out);
    } else {
      decompress(reader, out);
    }
  }

  private static boolean hasKey(Reader reader) {
    return reader.getKeyClass().isAssignableFrom(Text.class);
  }

  @SneakyThrows
  private static void decompressWithKey(Reader reader, BufferedWriter out) {
    val key = new Text();
    val value = new BytesWritable();
    while (reader.next(key, value)) {
      out.write(key.toString());
      out.write(TAB);
      val node = SMILE_READER.readValue(getBytes(value));
      out.write(node.toString());
      out.newLine();
    }
  }

  public static void decompress(Reader reader, BufferedWriter out) throws IOException {
    val key = NullWritable.get();
    val value = new BytesWritable();
    while (reader.next(key, value)) {
      val node = SMILE_READER.readValue(getBytes(value));
      out.write(node.toString());
      out.newLine();
    }
  }

  private static BufferedWriter getOutWriter(File outFile) throws IOException {
    return new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile), UTF_8));
  }

  @SneakyThrows
  private static InputStream getInputStream(File inFile) {
    return new SmileSequenceFileInputStream(new Configuration(), new Path(inFile.getAbsolutePath()));
  }

  @SneakyThrows
  private static SequenceFile.Reader getReader(File inFile) {
    val path = new Path(inFile.getAbsolutePath());

    return new SequenceFile.Reader(new Configuration(), Reader.file(path));
  }

}
