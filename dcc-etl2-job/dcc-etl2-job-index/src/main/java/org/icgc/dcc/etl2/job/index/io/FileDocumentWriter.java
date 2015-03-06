/*
 * Copyright (c) 2014 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.etl2.job.index.io;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkState;

import java.io.File;
import java.io.IOException;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl2.job.index.core.Document;
import org.icgc.dcc.etl2.job.index.core.DocumentWriter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Files;

@Slf4j
public class FileDocumentWriter implements DocumentWriter {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final File outputDir;

  public FileDocumentWriter(@NonNull File outputDir) {
    this.outputDir = outputDir;

    log.info("Output directory: '{}'", outputDir);
    checkState(outputDir.exists(), "Output directory %s does not exist. Aborting.", outputDir.getAbsolutePath());
  }

  @Override
  public void write(Document document) throws IOException {
    val file = resolveDocumentFile(document);
    val text = formatDocument(document);

    Files.write(text, file, UTF_8);
  }

  @Override
  public void close() throws IOException {
    log.info("Closing");
  }

  private File resolveDocumentFile(Document document) {
    val typeDir = getDocumentTypeDir(document);
    if (!typeDir.exists()) {
      checkState(typeDir.mkdir(), "Could not create type directory '%s'", typeDir);
    }

    val fileName = getDocumentFileName(document);
    int hashcode = fileName.hashCode();
    int mask = 255;

    val typeLeve1Dir = new File(typeDir, String.format("%02x", hashcode & mask));
    if (!typeLeve1Dir.exists()) {
      checkState(typeLeve1Dir.mkdir(), "Could not create dir1 directory '%s'", typeLeve1Dir);
    }

    val typeLevel2Dir = new File(typeLeve1Dir, String.format("%02x", (hashcode >> 8) & mask));
    if (!typeLevel2Dir.exists()) {
      checkState(typeLevel2Dir.mkdir(), "Could not create dir1 directory '%s'", typeLevel2Dir);
    }

    return new File(typeLevel2Dir, fileName);
  }

  private File getDocumentTypeDir(Document document) {
    return new File(outputDir, document.getType().getName());
  }

  private String getDocumentFileName(Document document) {
    return String.format("%s.json", document.getId());
  }

  private String formatDocument(Document document) throws JsonProcessingException {
    return MAPPER.writeValueAsString(document.getSource());
  }

}
