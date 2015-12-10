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

import static com.google.common.base.Strings.isNullOrEmpty;

import java.util.Map;

import lombok.NonNull;
import lombok.val;
import lombok.experimental.UtilityClass;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaRDD;

@UtilityClass
public class Configurations {

  public static void addCompressionCodec(@NonNull JobConf conf, Class<? extends CompressionCodec> codecClass) {
    val codecsProperty = "io.compression.codecs";
    val currentCodecs = conf.get(codecsProperty);
    val codecs = codecClass.getName() + (isNullOrEmpty(currentCodecs) ? "" : "," + currentCodecs);

    conf.set(codecsProperty, codecs);
  }

  public static void setAll(@NonNull Configuration conf, @NonNull Map<String, String> properties) {
    for (val entry : properties.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
  }

  public static JobConf createJobConf(@NonNull JavaRDD<?> rdd) {
    return new JobConf(rdd.context().hadoopConfiguration());
  }

}
