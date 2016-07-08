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
package org.icgc.dcc.release.core.hadoop;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.release.core.util.JacksonFactory.READER;

import java.io.IOException;

import lombok.val;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class SmileSequenceFileInputStreamTest {

  private static final String INPUT_PATH = "src/test/resources/fixtures/data.seq";

  FileSystem fileSystem;
  SmileSequenceFileInputStream inputStream;

  @Before
  public void setUp() throws IOException {
    fileSystem = FileSystem.getLocal(new Configuration());
    val file = new Path(INPUT_PATH);
    inputStream = new SmileSequenceFileInputStream(fileSystem.getConf(), file);
  }

  @After
  public void tearDown() throws IOException {
    inputStream.close();
  }

  @Test
  public void readTest() throws IOException {
    val iterator = READER.<ObjectNode> readValues(inputStream);
    while (iterator.hasNext()) {
      val value = iterator.next();
      assertThat(value.isObject()).isTrue();
    }
  }

}
