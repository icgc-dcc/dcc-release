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

import static com.google.common.collect.Maps.newLinkedHashMap;

import java.util.Map;

import lombok.Data;

import org.icgc.dcc.release.core.config.SnpEffProperties;
import org.icgc.dcc.release.job.document.config.DocumentProperties;
import org.icgc.dcc.release.job.export.config.HBaseProperties;
import org.icgc.dcc.release.job.id.config.IdProperties;
import org.icgc.dcc.release.job.imports.config.MongoProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class WorkflowProperties {

  @Bean
  @ConfigurationProperties(prefix = "mongo")
  public MongoProperties mongoProperties() {
    return new MongoProperties();
  }

  @Bean
  @ConfigurationProperties(prefix = "hbase")
  public HBaseProperties hbaseProperties() {
    return new HBaseProperties();
  }

  @Bean
  @ConfigurationProperties(prefix = "snpeff")
  public SnpEffProperties snpEffProperties() {
    return new SnpEffProperties();
  }

  @Bean
  @ConfigurationProperties(prefix = "index")
  public DocumentProperties indexProperties() {
    return new DocumentProperties();
  }

  @Bean
  @ConfigurationProperties(prefix = "spark")
  public SparkProperties sparkProperties() {
    return new SparkProperties();
  }

  @Bean
  @ConfigurationProperties(prefix = "hadoop")
  public HadoopProperties hadoopProperties() {
    return new HadoopProperties();
  }

  @Bean
  @ConfigurationProperties(prefix = "mail")
  public MailProperties mailProperties() {
    return new MailProperties();
  }

  @Bean
  @ConfigurationProperties(prefix = "dcc.id")
  public IdProperties identifierProperties() {
    return new IdProperties();
  }

  @Data
  public static class SparkProperties {

    private String master;
    private Map<String, String> properties = newLinkedHashMap();

  }

  @Data
  public static class HadoopProperties {

    private Map<String, String> properties = newLinkedHashMap();

  }

  @Data
  public static class MailProperties {

    private String recipients;
    private Map<String, String> properties = newLinkedHashMap();

  }

}
