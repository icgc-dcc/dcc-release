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

import static com.google.common.base.Stopwatch.createStarted;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_SERIALIZATIONS_KEY;
import static org.apache.hadoop.io.SequenceFile.Writer.compression;
import static org.apache.hadoop.io.SequenceFile.Writer.file;
import static org.apache.hadoop.io.SequenceFile.Writer.keyClass;
import static org.apache.hadoop.io.SequenceFile.Writer.valueClass;
import static org.icgc.dcc.release.core.function.JsonNodes.MAPPER;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import lombok.Cleanup;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.junit.Ignore;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Slf4j
@Ignore("prototyping")
public class ObjectNodeSerializationTest {

  @Test
  public void testSerialize() throws IOException {
    @Cleanup
    val writer = createWriter(new Path("/tmp/test.seq"));
    val iterator = readInput();

    val watch = createStarted();
    while (iterator.hasNext()) {
      val value = iterator.next();
      writer.append(NullWritable.get(), value);
    }

    log.info("Finished in {}", watch);
  }

  private Iterator<ObjectNode> readInput() throws IOException, JsonProcessingException {
    val reader = MAPPER.reader(ObjectNode.class);
    return reader.readValues(new File("/tmp/Data/Genes/gene.json"));
  }

  private Writer createWriter(Path path) throws IOException {
    val conf = new Configuration();
    conf.set(IO_SERIALIZATIONS_KEY,
        WritableSerialization.class.getName() + "," + ObjectNodeSerialization.class.getName());

    return SequenceFile.createWriter(conf,
        file(path),
        keyClass(NullWritable.class),
        valueClass(ObjectNode.class),
        compression(CompressionType.BLOCK));
  }

}
