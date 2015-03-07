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
package org.icgc.dcc.etl2.workflow.config;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_SERIALIZATIONS_KEY;
import static scala.collection.JavaConversions.asScalaMap;

import java.io.IOException;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.icgc.dcc.etl2.core.hadoop.ObjectNodeSerialization;
import org.icgc.dcc.etl2.core.job.Job;
import org.icgc.dcc.etl2.job.export.config.HBaseProperties;
import org.icgc.dcc.etl2.workflow.config.WorkflowProperties.HadoopProperties;
import org.icgc.dcc.etl2.workflow.config.WorkflowProperties.SparkProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * Spark configuration.
 * <p>
 * See annotation documentation for details.
 */
@Slf4j
@Configuration
public class SparkConfig {

  /**
   * Dependencies.
   */
  @Autowired
  private SparkProperties spark;
  @Autowired
  private HadoopProperties hadoop;
  @Autowired
  private HBaseProperties hbase;

  @Bean
  public SparkConf sparkConf() {
    log.info("Creating SparkConf with spark properties '{}'", spark);
    return new SparkConf()
        .setAppName("dcc-etl-workflow")
        .setMaster(spark.getMaster())
        .setAll(asScalaMap(spark.getProperties()));
  }

  @Bean(destroyMethod = "stop")
  public JavaSparkContext sparkContext() {
    log.info("Creating JavaSparkContext with hadoop properties '{}'", hadoop);
    val sparkContext = new JavaSparkContext(sparkConf());

    val jobJar = getJobJar();
    log.info("Adding job jar: {}", jobJar);
    sparkContext.addJar(jobJar);

    val configuration = sparkContext.hadoopConfiguration();
    for (val entry : hadoop.getProperties().entrySet()) {
      configuration.set(entry.getKey(), entry.getValue());
    }

    // Custom serialization needed for outputs / inputs
    configuration.set(IO_SERIALIZATIONS_KEY,
        WritableSerialization.class.getName() + "," + ObjectNodeSerialization.class.getName());

    return sparkContext;
  }

  @Bean
  @Primary
  public org.apache.hadoop.conf.Configuration hadoopConf() {
    return sparkContext().hadoopConfiguration();
  }

  @Bean
  public org.apache.hadoop.conf.Configuration hbaseConf() {
    val config = HBaseConfiguration.create(hadoopConf());
    for (val entry : hbase.getProperties().entrySet()) {
      config.set(entry.getKey(), entry.getValue());
    }

    return config;
  }

  @Bean
  public FileSystem fileSystem() throws IOException {
    return FileSystem.get(hadoopConf());
  }

  private String getJobJar() {
    val jobJarAnchor = Job.class;
    val path = jobJarAnchor.getProtectionDomain().getCodeSource().getLocation().getPath();
    val ide = path.contains("classes");

    return ide ? getClass().getProtectionDomain().getCodeSource().getLocation().getPath() : path;
  }

}
