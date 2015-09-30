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
package org.icgc.dcc.release.job.index.io;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;

public class FilteredOutputStream extends OutputStream {

  private final OutputStream headerStream;
  private final OutputStream dataStream;

  public FilteredOutputStream(@NonNull File headerFile, @NonNull File dataFile) {
    this.headerStream = createOutputStream(headerFile);
    this.dataStream = createOutputStream(dataFile);
  }

  @Override
  public void write(int b) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void write(byte[] b) throws IOException {
    val line = new String(b);
    if (isHeader(line)) {
      headerStream.write(b);
    } else {
      dataStream.write(b);
    }
  }

  @Override
  public void close() throws IOException {
    headerStream.close();
    dataStream.close();
    super.close();
  }

  private static boolean isHeader(String line) {
    return line.startsWith("#");
  }

  @SneakyThrows
  private OutputStream createOutputStream(File file) {
    return new BufferedOutputStream(new FileOutputStream(file));
  }

}
