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

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;

@Slf4j
public class DataTypesTest {

  public static final String INPUT_PATH = "src/test/resources/fixtures/loader";
  SparkConf sparkConf;

  @Before
  public void setUp() {
    val sparkConf = new SparkConf().setAppName("test").setMaster("local");
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.set("spark.kryo.registrator", "org.icgc.dcc.etl2.core.util.CustomKryoRegistrator");
    sparkConf.set("spark.task.maxFailures", "0");

  }

  @Test
  public void testSSMControlledDataType() {
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
    val dataType = new SSMControlledDataType(jsc);
    val input = readFile(jsc, INPUT_PATH + "/ssm/ALL-US/part-000000.json");
    val output = dataType.process(input);
    output(output);
  }

  @Test
  public void testSSMOpenDataType() {
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
    val dataType = new SSMOpenDataType(jsc);
    val input = readFile(jsc, INPUT_PATH + "/ssm/ALL-US/part-000000.json");
    val output = dataType.process(input);
    output(output);
  }

  private void output(JavaRDD<ObjectNode> results) {
    log.info("{}", results.count());
    results.foreach(result -> log.info("{}", result.toString()));
  }

  private JavaRDD<String> readFile(JavaSparkContext jsc, String path) {
    return jsc.textFile(path);
  }

}
