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
package org.icgc.dcc.etl2.core.hadoop;

import static org.icgc.dcc.etl2.core.function.JsonNodes.$;

import java.io.IOException;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.junit.Test;

@Slf4j
public class ObjectNodeWritableTest {

  @Test
  public void testWritable() throws IOException {
    // Data
    val value1 = $("{x:1}");
    val value2 = $("{x:2}");
    val value3 = $("{x:3}");

    // Write
    val out = new DataOutputBuffer();
    val from1 = new ObjectNodeWritable(value1);
    from1.write(out);
    val from2 = new ObjectNodeWritable(value2);
    from2.write(out);
    val from3 = new ObjectNodeWritable(value3);
    from3.write(out);

    // Read
    val in = new DataInputBuffer();
    in.reset(out.getData(), out.getLength());
    val to1 = new ObjectNodeWritable();
    log.info("{}", to1.get());
    to1.readFields(in);
    val to2 = new ObjectNodeWritable();
    to2.readFields(in);
    val to3 = new ObjectNodeWritable();
    to3.readFields(in);

    // Compare
    log.info("{}", to2.get());
    log.info("{}", to3.get());
  }

}
