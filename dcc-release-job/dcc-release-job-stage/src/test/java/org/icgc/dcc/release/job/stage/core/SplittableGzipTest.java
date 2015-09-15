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
package org.icgc.dcc.release.job.stage.core;

import lombok.val;
import lombok.extern.slf4j.Slf4j;
import nl.basjes.hadoop.io.compress.SplittableGzipCodec;

import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.icgc.dcc.release.core.util.Configurations;
import org.icgc.dcc.release.core.util.JavaRDDs;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.context.annotation.Bean;

@Slf4j
@Ignore("prototyping")
public class SplittableGzipTest {

  @Test
  public void testSplitUncompressed() {
    val sparkContext = new JavaSparkContext(sparkConf());

    val conf = new JobConf(sparkContext.hadoopConfiguration());
    val splitSize = Long.toString(128L * 1024L * 1024L);
    conf.set("mapred.min.split.size", splitSize);
    conf.set("mapred.max.split.size", splitSize);

    val path = "/icgc/submission/ICGC18/BRCA-US/meth_array_p.20141111.txt";

    val rdd = JavaRDDs.combineTextFile(sparkContext, path + "," + path, conf);
    JavaRDDs.logPartitions(log, rdd.partitions());
  }

  @Test
  public void testSplitCompressed() {
    val sparkContext = new JavaSparkContext(sparkConf());

    val conf = new JobConf(sparkContext.hadoopConfiguration());
    val splitSize = Long.toString(128L * 1024L * 1024L);
    conf.set("mapred.min.split.size", splitSize);
    conf.set("mapred.max.split.size", splitSize);

    Configurations.addCompressionCodec(conf, SplittableGzipCodec.class);

    val path = "/tmp/meth_seq_p.txt.gz";

    val rdd = JavaRDDs.combineTextFile(sparkContext, path, conf);
    JavaRDDs.logPartitions(log, rdd.partitions());
  }

  @Bean
  public SparkConf sparkConf() {
    return new SparkConf()
        .setAppName(SplittableGzipTest.class.getSimpleName())
        .setMaster("local")
        .set("spark.hadoop.fs.defaultFS", "***REMOVED***");
  }

}
