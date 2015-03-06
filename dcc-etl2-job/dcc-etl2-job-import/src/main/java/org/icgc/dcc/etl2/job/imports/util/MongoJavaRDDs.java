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
package org.icgc.dcc.etl2.job.imports.util;

import static lombok.AccessLevel.PRIVATE;
import lombok.NoArgsConstructor;
import lombok.val;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.icgc.dcc.etl2.job.imports.hadoop.MongoAdminInputFormat;
import org.icgc.dcc.etl2.job.imports.hadoop.MongoWritables;

import scala.Tuple2;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.hadoop.MongoConfig;
import com.mongodb.hadoop.io.BSONWritable;

@NoArgsConstructor(access = PRIVATE)
public final class MongoJavaRDDs {

  public static JavaRDD<ObjectNode> javaMongoCollection(JavaSparkContext context, MongoConfig mongoConfig,
      JobConf hadoopConf) {
    // See https://groups.google.com/d/topic/cascading-user/ngLidsZQjIU/discussion
    FileInputFormat.addInputPaths(hadoopConf, mongoConfig.getInputURI().toString());

    // See https://jira.mongodb.org/browse/HADOOP-62
    hadoopConf.setInputFormat(MongoAdminInputFormat.class);

    val hadoopRDD = context.hadoopRDD(hadoopConf, MongoAdminInputFormat.class, BSONWritable.class, BSONWritable.class,
        context.defaultMinPartitions());

    return hadoopRDD.map(new ConvertBSONWritableValue());
  }

  private static class ConvertBSONWritableValue implements Function<Tuple2<BSONWritable, BSONWritable>, ObjectNode> {

    @Override
    public ObjectNode call(Tuple2<BSONWritable, BSONWritable> tuple) throws Exception {
      return MongoWritables.convertBSONWritable(tuple._2);
    }

  }

}
