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

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.core.json.Jackson.toObjectNode;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import lombok.val;

import org.junit.Before;
import org.junit.Test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class SmileSerializerTest {

  SmileSerializer smileSerializer;
  Kryo kryo;

  @Before
  public void setUp() {
    smileSerializer = new SmileSerializer();
    kryo = new Kryo();
  }

  @Test
  public void roundTripTest() throws Exception {
    val outStream = new ByteArrayOutputStream();
    val output = new Output(outStream, 32);
    val sourceObj1 = toObjectNode("{\"a\":1}");
    val sourceObj3 = toObjectNode("{\"a\":12}");
    val sourceObj4 = toObjectNode("{\"a\":555}");

    smileSerializer.write(kryo, output, sourceObj1);
    smileSerializer.write(kryo, output, null);
    smileSerializer.write(kryo, output, sourceObj3);
    smileSerializer.write(kryo, output, sourceObj4);
    output.flush();

    val input = new Input(new ByteArrayInputStream(outStream.toByteArray()), 32);
    val targetObj1 = smileSerializer.read(kryo, input, ObjectNode.class);
    val targetObj2 = smileSerializer.read(kryo, input, ObjectNode.class);
    val targetObj3 = smileSerializer.read(kryo, input, ObjectNode.class);
    val targetObj4 = smileSerializer.read(kryo, input, ObjectNode.class);

    assertThat(sourceObj1).isEqualTo(targetObj1);
    assertThat(targetObj2).isNull();
    assertThat(sourceObj3).isEqualTo(targetObj3);
    assertThat(sourceObj4).isEqualTo(targetObj4);

  }

}
