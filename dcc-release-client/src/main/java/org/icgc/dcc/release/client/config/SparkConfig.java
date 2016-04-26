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
package org.icgc.dcc.release.client.config;

import static org.icgc.dcc.release.client.util.Names.APPLICATION_BASE_NAME;
import static scala.collection.JavaConversions.asScalaMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.icgc.dcc.release.client.config.WorkflowProperties.SparkProperties;
import org.icgc.dcc.release.core.job.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * Spark configuration.
 * <p>
 * See annotation documentation for details.
 */
@Slf4j
@Lazy
@Configuration
public class SparkConfig {

  /**
   * Dependencies.
   */
  @Autowired
  SparkProperties spark;

  @Bean
  public SparkConf sparkConf() {
    log.info("Creating SparkConf with spark properties '{}'", spark);
    return new SparkConf()
        .setAppName(APPLICATION_BASE_NAME + "-workflow")
        .setMaster(spark.getMaster())
        .setAll(asScalaMap(spark.getProperties()));
  }

  @Bean(destroyMethod = "stop")
  public JavaSparkContext sparkContext() {
    log.info("Creating JavaSparkContext for application id {}...", sparkConf().getAppId());
    val sparkContext = new JavaSparkContext(sparkConf());

    val jobJar = getJobJar();
    log.info("Adding job jar: {}", jobJar);
    sparkContext.addJar(jobJar);

    return sparkContext;
  }

  private String getJobJar() {
    val jobJarAnchor = Job.class;
    val path = getPath(jobJarAnchor);

    return isExpoded(path) ? getPath(getClass()) : path;
  }

  private static boolean isExpoded(String path) {
    return path.contains("classes");
  }

  private static String getPath(Class<?> type) {
    return type.getProtectionDomain().getCodeSource().getLocation().getPath();
  }

}
