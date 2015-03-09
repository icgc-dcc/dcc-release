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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.spark.api.java.JavaHadoopRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.icgc.dcc.etl2.core.hadoop.CombineTextInputFormat;

@UtilityClass
public class JavaRDDs {

  @NonNull
  public static JavaHadoopRDD<LongWritable, Text> textFile(JavaSparkContext sparkContext, String paths) {
    return textFile(sparkContext, paths, createJobConf(sparkContext));
  }

  @NonNull
  public static JavaHadoopRDD<LongWritable, Text> textFile(JavaSparkContext sparkContext, String paths, JobConf conf) {
    TextInputFormat.setInputPaths(conf, new Path(paths));
    val hadoopRDD = sparkContext.hadoopRDD(conf, TextInputFormat.class, LongWritable.class, Text.class,
        sparkContext.defaultMinPartitions());

    return (JavaHadoopRDD<LongWritable, Text>) hadoopRDD;
  }

  @NonNull
  public static <K, V> JavaHadoopRDD<K, V> sequenceFile(JavaSparkContext sparkContext, String paths,
      Class<K> keyClass, Class<V> valueClass) {
    return sequenceFile(sparkContext, paths, keyClass, valueClass, createJobConf(sparkContext));
  }

  @NonNull
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static <K, V> JavaHadoopRDD<K, V> sequenceFile(JavaSparkContext sparkContext, String paths, Class<K> keyClass,
      Class<V> valueClass, JobConf conf) {
    SequenceFileInputFormat.setInputPaths(conf, paths);

    val hadoopRDD = sparkContext.hadoopRDD(conf, SequenceFileInputFormat.class, keyClass, valueClass,
        sparkContext.defaultMinPartitions());

    return (JavaHadoopRDD<K, V>) hadoopRDD;
  }

  @NonNull
  public static JavaHadoopRDD<LongWritable, Text> combineTextFile(JavaSparkContext sparkContext, String paths) {
    return combineTextFile(sparkContext, paths, createJobConf(sparkContext));
  }

  @NonNull
  public static JavaHadoopRDD<LongWritable, Text> combineTextFile(JavaSparkContext sparkContext, String paths,
      JobConf conf) {
    CombineTextInputFormat.setInputPaths(conf, paths);
    val hadoopRDD = sparkContext.hadoopRDD(conf, CombineTextInputFormat.class, LongWritable.class, Text.class,
        sparkContext.defaultMinPartitions());

    return (JavaHadoopRDD<LongWritable, Text>) hadoopRDD;
  }

  @NonNull
  public static <K, V> void saveAsSequenceFile(JavaPairRDD<K, V> rdd, Class<K> keyClass, Class<V> valueClass,
      String path) {
    rdd.saveAsHadoopFile(path, keyClass, valueClass, SequenceFileOutputFormat.class, createJobConf(rdd));
  }

  @NonNull
  public static <K, V> void saveAsSequenceFile(JavaPairRDD<K, V> rdd, Class<K> keyClass, Class<V> valueClass,
      String path, JobConf conf) {

    // Compress
    SequenceFileOutputFormat.setCompressOutput(conf, true);
    SequenceFileOutputFormat.setOutputCompressionType(conf, CompressionType.BLOCK);
    SequenceFileOutputFormat.setOutputCompressorClass(conf, SnappyCodec.class);

    rdd.saveAsHadoopFile(path, keyClass, valueClass, SequenceFileOutputFormat.class, conf);
  }

  private static JobConf createJobConf(JavaPairRDD<?, ?> rdd) {
    return new JobConf(rdd.context().hadoopConfiguration());
  }

  private static JobConf createJobConf(JavaSparkContext sparkContext) {
    return new JobConf(sparkContext.hadoopConfiguration());
  }

}
