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

import lombok.NonNull;
import lombok.val;
import lombok.experimental.UtilityClass;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.icgc.dcc.etl2.core.function.FormatObjectNode;
import org.icgc.dcc.etl2.core.function.ParseObjectNode;

import scala.Tuple2;

import com.fasterxml.jackson.databind.node.ObjectNode;

@UtilityClass
public class ObjectNodeRDDs {

  @NonNull
  public static JavaRDD<ObjectNode> textObjectNodeFile(JavaSparkContext sparkContext, String path) {
    return textObjectNodeFile(sparkContext, path, createJobConf(sparkContext));
  }

  @NonNull
  public static JavaRDD<ObjectNode> textObjectNodeFile(JavaSparkContext sparkContext, String path, JobConf conf) {
    return JavaRDDs.textFile(sparkContext, path, conf)
        .map(tuple -> tuple._2.toString())
        .map(new ParseObjectNode());
  }

  @NonNull
  public static JavaRDD<ObjectNode> sequenceObjectNodeFile(JavaSparkContext sparkContext, String path) {
    return sequenceObjectNodeFile(sparkContext, path, createJobConf(sparkContext));
  }

  @NonNull
  public static JavaRDD<ObjectNode> sequenceObjectNodeFile(JavaSparkContext sparkContext, String path, JobConf conf) {
    return JavaRDDs.sequenceFile(sparkContext, path, NullWritable.class, ObjectNode.class)
        .map(tuple -> tuple._2);
  }

  @NonNull
  public static JavaRDD<ObjectNode> combineObjectNodeFile(JavaSparkContext sparkContext, String paths) {
    return combineObjectNodeFile(sparkContext, paths, createJobConf(sparkContext));
  }

  @NonNull
  public static JavaRDD<ObjectNode> combineObjectNodeFile(JavaSparkContext sparkContext, String paths, JobConf conf) {
    return JavaRDDs.combineTextFile(sparkContext, paths, conf)
        .map(tuple -> tuple._2.toString())
        .map(new ParseObjectNode());
  }

  @NonNull
  public static void saveAsTextFile(JavaRDD<ObjectNode> rdd, String path) {
    val output = rdd.map(new FormatObjectNode());
    output.saveAsTextFile(path);
  }

  @NonNull
  public static void saveAsSequenceFile(JavaRDD<ObjectNode> rdd, String path) {
    val conf = createJobConf(rdd);
    saveAsSequenceFile(rdd, path, conf);
  }

  @NonNull
  public static void saveAsSequenceFile(JavaRDD<ObjectNode> rdd, String path, JobConf conf) {
    val pairRdd = rdd.mapToPair(row -> new Tuple2<NullWritable, ObjectNode>(NullWritable.get(), row));
    JavaRDDs.saveAsSequenceFile(pairRdd, NullWritable.class, ObjectNode.class, path, conf);
  }

  private static JobConf createJobConf(JavaRDD<?> rdd) {
    return new JobConf(rdd.context().hadoopConfiguration());
  }

  private static JobConf createJobConf(JavaSparkContext sparkContext) {
    return new JobConf(sparkContext.hadoopConfiguration());
  }

}
