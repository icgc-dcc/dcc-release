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
package org.icgc.dcc.etl2.job.export.model.type;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;

import lombok.val;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.icgc.dcc.etl2.test.job.AbstractJobTest;
import org.junit.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class DataTypesTest extends AbstractJobTest {

  private static final String INPUT_PATH = "src/test/resources/fixtures/data/";

  @Test
  public void testNoConsequenceClinicalDataType() {
    val dataType = new Clinical();
    val input = sparkContext.textFile(INPUT_PATH + "clinical_nc.json");
    val output = dataType.process(input);
    output.foreach(line -> System.out.println(line.toString()));

    val result = output.collect();
    val json = result.get(0);
    assertThat(result.size()).isEqualTo(1);
    assert (areEqual(getFieldNames(json), dataType.getFields()));
  }

  @Test
  public void testMultiConsequenceClinicalDataType() {
    val dataType = new Clinical();
    val input = sparkContext.textFile(INPUT_PATH + "clinical_mc.json");
    val output = dataType.process(input);
    output.foreach(line -> System.out.println(line.toString()));

    val result = output.collect();
    val json = result.get(0);
    assertThat(result.size()).isEqualTo(2);
    assert (areEqual(getFieldNames(json), dataType.getFields()));
  }

  @Test
  public void testMultiConsequenceSSMControlledDataType() {
    val dataType = new SSMControlled();
    val input = readFile(sparkContext, INPUT_PATH + "ssm_controlled_mc.json");
    val output = dataType.process(input);
    output.foreach(line -> System.out.println(line.toString()));

    val result = output.collect();
    val json = result.get(0);
    assertThat(result.size()).isEqualTo(2);
    assert (areEqual(getFieldNames(json), dataType.getFields()));
  }

  @Test
  public void testNoConsequenceSSMControlledDataType() {
    val dataType = new SSMControlled();
    val input = readFile(sparkContext, INPUT_PATH + "ssm_controlled_nc.json");
    val output = dataType.process(input);
    output.foreach(line -> System.out.println(line.toString()));

    val result = output.collect();
    val json = result.get(0);
    assertThat(result.size()).isEqualTo(1);
    assert (areEqual(getFieldNames(json), dataType.getFields()));
  }

  @Test
  public void testNoConsequenceSSMOpenDataType() {
    val dataType = new SSMOpen();
    val input = readFile(sparkContext, INPUT_PATH + "ssm_open_nc.json");
    val output = dataType.process(input);
    output.foreach(line -> System.out.println(line.toString()));

    val result = output.collect();
    val json = result.get(0);
    assertThat(result.size()).isEqualTo(1);
    assert (areEqual(getFieldNames(json), dataType.getFields()));
  }

  @Test
  public void testMultiConsequenceSSMOpenDataType() {
    val dataType = new org.icgc.dcc.etl2.job.export.model.type.SSMOpen();
    val input = readFile(sparkContext, INPUT_PATH + "ssm_open_mc.json");
    val output = dataType.process(input);
    output.foreach(line -> System.out.println(line.toString()));

    val result = output.collect();
    val json = result.get(0);
    assertThat(result.size()).isEqualTo(2);
    assert (areEqual(getFieldNames(json), dataType.getFields()));
  }

  @Test
  public void testNoConsequenceSGVControlledDataType() {
    val dataType = new SGVControlled();
    val input = readFile(sparkContext, INPUT_PATH + "sgv_nc.json");
    val output = dataType.process(input);
    output.foreach(line -> System.out.println(line.toString()));

    val result = output.collect();
    val json = result.get(0);
    assertThat(result.size()).isEqualTo(1);
    assert (areEqual(getFieldNames(json), dataType.getFields()));
  }

  private JavaRDD<String> readFile(JavaSparkContext sparkContext, String path) {
    return sparkContext.textFile(path);
  }

  private Set<String> getFieldNames(ObjectNode json) {
    val result = ImmutableSet.<String> builder();
    val itr = json.fieldNames();
    while (itr.hasNext()) {
      result.add(itr.next());
    }
    return result.build();
  }

  private boolean areEqual(Set<String> a, Set<String> b) {
    return Sets.difference(a, b).isEmpty();
  }

}
