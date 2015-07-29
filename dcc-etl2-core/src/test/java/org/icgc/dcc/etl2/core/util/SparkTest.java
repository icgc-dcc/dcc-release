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
package org.icgc.dcc.etl2.core.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.etl2.core.util.Tuples.tuple;

import java.util.Collections;

import lombok.val;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

public class SparkTest {

  @Test
  public void joinEmptyRDDTest() {
    val sparkConf = new SparkConf().setAppName("test").setMaster("local");

    try (val sparkContext = new JavaSparkContext(sparkConf)) {
      val oneRdd = sparkContext.parallelize(Collections.singletonList("one"));
      val twoRdd = sparkContext.parallelize(Collections.singletonList("two"));
      val threeRdd = sparkContext.emptyRDD();

      val onePair = oneRdd.mapToPair(t -> tuple(1, t));
      val twoPair = twoRdd.groupBy(t -> 1);
      val threePair = threeRdd.groupBy(t -> 1);

      assertThat(onePair.leftOuterJoin(twoPair).collect()).isNotEmpty();

      // FIXME: https://issues.apache.org/jira/browse/SPARK-9236
      assertThat(onePair.leftOuterJoin(threePair).collect()).isEmpty();
    }
  }

}
