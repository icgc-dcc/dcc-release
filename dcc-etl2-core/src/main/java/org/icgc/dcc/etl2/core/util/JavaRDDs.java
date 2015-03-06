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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.spark.api.java.JavaHadoopRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.icgc.dcc.etl2.core.function.ExtractPairValue;
import org.icgc.dcc.etl2.core.function.ParseObjectNode;
import org.icgc.dcc.etl2.core.hadoop.CombineTextInputFormat;

import com.fasterxml.jackson.databind.node.ObjectNode;

@UtilityClass
public class JavaRDDs {

  @NonNull
  public static JavaHadoopRDD<LongWritable, Text> javaHadoopRDD(JavaSparkContext sparkContext, String paths) {
    return (JavaHadoopRDD<LongWritable, Text>) sparkContext.hadoopFile(
        paths, TextInputFormat.class, LongWritable.class, Text.class);
  }

  @NonNull
  public static JavaRDD<String> javaTextFile(JavaSparkContext sparkContext, String paths, JobConf hadoopConf) {
    TextInputFormat.setInputPaths(hadoopConf, new Path(paths));
    val hadoopRDD = sparkContext.hadoopRDD(hadoopConf, TextInputFormat.class, LongWritable.class, Text.class,
        sparkContext.defaultMinPartitions());

    return hadoopRDD.map(tuple -> tuple._2.toString());
  }

  @NonNull
  public static JavaRDD<ObjectNode> javaTextObjectNodeRDD(JavaSparkContext sparkContext, String path) {
    return sparkContext
        .textFile(path)
        .map(new ParseObjectNode());
  }

  @NonNull
  public static JavaRDD<ObjectNode> javaSequenceObjectNodeRDD(JavaSparkContext sparkContext, String path) {
    return sparkContext
        .sequenceFile(path, NullWritable.class, ObjectNode.class)
        .map(new ExtractPairValue<NullWritable, ObjectNode>());
  }

  @NonNull
  public static JavaRDD<String> javaCombineTextFile(JavaSparkContext sparkContext, String paths, JobConf hadoopConf) {
    CombineTextInputFormat.setInputPaths(hadoopConf, new Path(paths));
    val hadoopRDD = sparkContext.hadoopRDD(hadoopConf, CombineTextInputFormat.class, LongWritable.class, Text.class,
        sparkContext.defaultMinPartitions());

    return hadoopRDD.map(tuple -> tuple._2.toString());
  }

}
