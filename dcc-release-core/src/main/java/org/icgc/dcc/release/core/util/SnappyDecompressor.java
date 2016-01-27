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

import static com.google.common.base.Preconditions.checkArgument;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.val;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.release.core.hadoop.SmileSequenceFileInputStream;

public class SnappyDecompressor {

  private static final int DEFAULT_BUFFER_SIZE = 32768;

  public static void main(String... args) {
    checkArgument(args.length == 1, "Expected input file");
    val inFile = new File(args[0]);
    checkArgument(inFile.canRead(), "Can't read file %s", inFile);

    new SnappyDecompressor().execute(inFile);
  }

  @SneakyThrows
  public void execute(File inFile) {
    @Cleanup
    val in = getInputStream(inFile);
    @Cleanup
    val out = getOutStream(inFile);
    decompress(in, out);
  }

  public static void decompress(InputStream in, OutputStream out) throws IOException {
    byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
    int len = in.read(buffer);
    while (len != -1) {
      out.write(buffer, 0, len);
      len = in.read(buffer);
    }
  }

  private FileOutputStream getOutStream(File inFile) throws FileNotFoundException {
    val outFile = new File("/tmp/" + inFile.getName() + ".out");

    return new FileOutputStream(outFile);
  }

  @SneakyThrows
  private InputStream getInputStream(File inFile) {
    return new SmileSequenceFileInputStream(new Configuration(), new Path(inFile.getAbsolutePath()));
  }

}
