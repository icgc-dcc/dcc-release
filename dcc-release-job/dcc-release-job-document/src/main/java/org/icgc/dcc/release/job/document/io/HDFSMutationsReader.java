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
package org.icgc.dcc.release.job.document.io;

import static org.icgc.dcc.release.core.util.ObjectNodes.MAPPER;

import java.io.InputStream;
import java.util.Iterator;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.release.core.hadoop.FileGlobInputStream;
import org.icgc.dcc.release.core.job.FileType;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ObjectNode;

@RequiredArgsConstructor
public class HDFSMutationsReader {

  @NonNull
  private final String workingDir;
  @NonNull
  private final FileSystem fileSystem;

  /**
   * Constants.
   */
  private static final ObjectReader READER = MAPPER.reader(ObjectNode.class);

  public Iterator<ObjectNode> createMutationsIterator() {
    val inputPath = new Path(workingDir, FileType.MUTATION_CENTRIC_DOCUMENT.getDirName());
    val inputStream = new FileGlobInputStream(fileSystem, inputPath);

    return readInput(inputStream);
  }

  @SneakyThrows
  private static Iterator<ObjectNode> readInput(InputStream inputStream) {
    return READER.<ObjectNode> readValues(inputStream);
  }

}
