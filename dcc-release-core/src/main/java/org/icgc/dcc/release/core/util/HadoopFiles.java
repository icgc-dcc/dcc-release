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

import static lombok.AccessLevel.PRIVATE;
import lombok.NoArgsConstructor;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.icgc.dcc.release.core.function.ParseObjectNode;

@NoArgsConstructor(access = PRIVATE)
public final class HadoopFiles {

  public static <T> JavaPairRDD<String, T> sequenceFileWithKey(JavaSparkContext sparkContext, String path,
      JobConf conf, Class<T> clazz) {
    return JavaRDDs.sequenceFile(sparkContext, path, Text.class, BytesWritable.class)
        .mapToPair(new ReadKeySequenceFile<T>(clazz));

  }

  public static <T> JavaRDD<T> sequenceFile(JavaSparkContext sparkContext, String path, JobConf conf, Class<T> clazz) {
    return JavaRDDs.sequenceFile(sparkContext, path, NullWritable.class, BytesWritable.class)
        .map(new ReadSequenceFile<T>(clazz));
  }

  public static <T> JavaRDD<T> textFile(JavaSparkContext sparkContext, String path, JobConf conf, Class<T> clazz) {
    return JavaRDDs.textFile(sparkContext, path, conf)
        .map(tuple -> tuple._2.toString())
        .map(new ParseObjectNode<T>(clazz));
  }

}
