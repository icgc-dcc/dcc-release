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

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_SERIALIZATIONS_KEY;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.icgc.dcc.release.client.config.WorkflowProperties.HadoopProperties;
import org.icgc.dcc.release.core.hadoop.ObjectNodeSerialization;
import org.icgc.dcc.release.core.util.Configurations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Primary;

/**
 * Hadoop configuration.
 * <p>
 * See annotation documentation for details.
 */
@Lazy
@org.springframework.context.annotation.Configuration
public class HadoopConfig {

  /**
   * Dependencies.
   */
  @Value("#{sparkContext.hadoopConfiguration()}")
  Configuration conf;
  @Autowired
  HadoopProperties hadoop;

  @Bean
  @Primary
  public Configuration hadoopConf() {
    Configurations.setAll(conf, hadoop.getProperties());

    // Custom serialization needed for outputs / inputs
    conf.set(IO_SERIALIZATIONS_KEY,
        WritableSerialization.class.getName() + "," + ObjectNodeSerialization.class.getName());

    return conf;
  }

  @Bean
  public FileSystem fileSystem() throws IOException {
    return FileSystem.get(hadoopConf());
  }

}
