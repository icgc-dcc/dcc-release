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
package org.icgc.dcc.release.core.util;

import static org.icgc.dcc.release.core.util.JacksonFactory.SMILE_READER;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.val;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.icgc.dcc.release.core.function.FormatObjectNode;
import org.icgc.dcc.release.core.function.ParseObjectNode;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ObjectNode;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ObjectNodeRDDs {

  @NonNull
  public static JavaRDD<ObjectNode> textObjectNodeFile(JavaSparkContext sparkContext, String path) {
    return textObjectNodeFile(sparkContext, path, createJobConf(sparkContext));
  }

  @NonNull
  public static JavaRDD<ObjectNode> textObjectNodeFile(JavaSparkContext sparkContext, String path, JobConf conf) {
    return JavaRDDs.textFile(sparkContext, path, conf)
        .map(tuple -> tuple._2.toString())
        .map(new ParseObjectNode<ObjectNode>());
  }

  public static <T> JavaRDD<T> textObjectNodeFile(JavaSparkContext sparkContext, String path, JobConf conf,
      Class<T> clazz) {
    return JavaRDDs.textFile(sparkContext, path, conf)
        .map(tuple -> tuple._2.toString())
        .map(new ParseObjectNode<T>(clazz));
  }

  @NonNull
  public static JavaRDD<ObjectNode> sequenceObjectNodeFile(JavaSparkContext sparkContext, String path) {
    return sequenceObjectNodeFile(sparkContext, path, createJobConf(sparkContext));
  }

  @NonNull
  public static JavaRDD<ObjectNode> sequenceObjectNodeFile(JavaSparkContext sparkContext, String path, JobConf conf) {
    return JavaRDDs.sequenceFile(sparkContext, path, NullWritable.class, BytesWritable.class)
        .map(tuple -> SMILE_READER.readValue(tuple._2.getBytes()));
  }

  public static <T> JavaRDD<T> sequenceObjectNodeFile(JavaSparkContext sparkContext, String path, JobConf conf,
      Class<T> clazz) {

    return JavaRDDs.sequenceFile(sparkContext, path, NullWritable.class, BytesWritable.class)
        .map(tuple -> {
          ObjectReader reader = JacksonFactory.SMILE_MAPPER.reader(clazz);

          return reader.readValue(tuple._2.getBytes());
        });
  }

  @NonNull
  public static JavaRDD<ObjectNode> combineObjectNodeFile(JavaSparkContext sparkContext, String paths) {
    return combineObjectNodeFile(sparkContext, paths, createJobConf(sparkContext));
  }

  @NonNull
  public static JavaRDD<ObjectNode> combineObjectNodeFile(JavaSparkContext sparkContext, String paths, JobConf conf) {
    return JavaRDDs.combineTextFile(sparkContext, paths, conf)
        .map(tuple -> tuple._2.toString())
        .map(new ParseObjectNode<ObjectNode>());
  }

  public static JavaRDD<ObjectNode> combineObjectNodeSequenceFile(@NonNull JavaSparkContext sparkContext,
      @NonNull String paths, @NonNull JobConf conf) {
    return JavaRDDs.combineSequenceFile(sparkContext, paths, conf)
        .map(tuple -> SMILE_READER.readValue(tuple._2.getBytes()));
  }

  public static void saveAsTextObjectNodeFile(@NonNull JavaRDD<ObjectNode> rdd, @NonNull String path) {
    saveAsTextObjectNodeFile(rdd, path, ObjectNode.class);
  }

  public static <T> void saveAsTextObjectNodeFile(@NonNull JavaRDD<T> rdd, @NonNull String path, @NonNull Class<T> clazz) {
    val output = rdd.map(new FormatObjectNode<T>(clazz));

    JavaRDDs.saveAsTextFile(output, path);
  }

  @NonNull
  public static void saveAsSequenceObjectNodeFile(JavaRDD<ObjectNode> rdd, String path) {
    val conf = Configurations.createJobConf(rdd);
    saveAsSequenceObjectNodeFile(rdd, path, conf);
  }

  public static <T> void saveAsSequenceObjectNodeFile(JavaRDD<T> rdd, String path, Class<T> clazz) {
    val conf = Configurations.createJobConf(rdd);
    saveAsSequenceObjectNodeFile(rdd, path, conf, clazz);
  }

  @NonNull
  public static void saveAsSequenceObjectNodeFile(JavaRDD<ObjectNode> rdd, String path, JobConf conf) {
    saveAsSequenceObjectNodeFile(rdd, path, conf, ObjectNode.class);
  }

  public static <T> void saveAsSequenceObjectNodeFile(JavaRDD<T> rdd, String path, JobConf conf, Class<T> clazz) {
    val pairRdd = rdd.mapToPair(new WriteObjectNode<T>(clazz));
    JavaRDDs.saveAsSequenceFile(pairRdd, NullWritable.class, BytesWritable.class, path, conf);
  }

  private static JobConf createJobConf(JavaSparkContext sparkContext) {
    return new JobConf(sparkContext.hadoopConfiguration());
  }

}
