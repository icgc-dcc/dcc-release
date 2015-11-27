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
package org.icgc.dcc.release.job.document.util;

import static com.google.common.base.Preconditions.checkState;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

import com.google.code.externalsorting.ExternalSort;

@RequiredArgsConstructor
public class VCFFileSorter {

  private static final int DEFAULT_BUFFER_SIZE = 25 * 1024 * 1024; // 25MB

  @NonNull
  private final File dataFile;
  @NonNull
  private final File headerFile;

  public void sortAndSave(@NonNull OutputStream outputStream) throws IOException {
    checkState(dataFile.canRead() && headerFile.canRead(), "Either header file %s or data file %s is not readable",
        headerFile, dataFile);
    val sorted = createTmpFile();
    ExternalSort.sort(dataFile, sorted);

    @Cleanup
    val headerStream = createInputStream(headerFile);
    copy(headerStream, outputStream);

    @Cleanup
    val dataStream = createInputStream(sorted);
    copy(dataStream, outputStream);
  }

  @SneakyThrows
  private void copy(InputStream inputStream, OutputStream outputStream) {
    byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
    int readBytes = 0;
    while ((readBytes = inputStream.read(buffer, 0, DEFAULT_BUFFER_SIZE)) != -1) {
      outputStream.write(buffer, 0, readBytes);
    }
  }

  private static File createTmpFile() throws IOException {
    val tmpFile = File.createTempFile("vcf-", ".sorted");
    tmpFile.deleteOnExit();

    return tmpFile;
  }

  @SneakyThrows
  private static InputStream createInputStream(File file) {
    return new BufferedInputStream(new FileInputStream(file));
  }

}
