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
import static org.icgc.dcc.release.core.util.JacksonFactory.READER;

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
import org.icgc.dcc.release.core.hadoop.SmileSequenceFileInputStream;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class SmileSequenceFileDecompressor {

  public static void main(String... args) {
    checkArgument(args.length == 2, "Expected input and output file");
    val inFile = new File(args[0]);
    val outFile = new File(args[1]);
    checkArgument(inFile.canRead(), "Can't read file %s", inFile);

    new SmileSequenceFileDecompressor().execute(inFile, outFile);
  }

  @SneakyThrows
  public void execute(File inFile, File outFile) {
    @Cleanup
    val in = getInputStream(inFile);
    @Cleanup
    val out = getOutWriter(outFile);
    decompress(in, out);
  }

  public static void decompress(InputStream in, BufferedWriter out) throws IOException {
    val iterator = READER.<ObjectNode> readValues(in);
    while (iterator.hasNext()) {
      val value = iterator.next();
      out.write(value.toString());
      out.newLine();
    }
  }

  private static BufferedWriter getOutWriter(File outFile) throws IOException {
    return new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile), UTF_8));
  }

  @SneakyThrows
  private InputStream getInputStream(File inFile) {
    return new SmileSequenceFileInputStream(new Configuration(), new Path(inFile.getAbsolutePath()));
  }

}
