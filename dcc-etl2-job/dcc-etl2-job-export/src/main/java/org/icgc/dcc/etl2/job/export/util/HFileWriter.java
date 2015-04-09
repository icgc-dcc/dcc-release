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
package org.icgc.dcc.etl2.job.export.util;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.icgc.dcc.etl2.job.export.function.TranslateHBaseKeyValue;

import com.fasterxml.jackson.databind.node.ObjectNode;

@RequiredArgsConstructor
public class HFileWriter {

  /**
   * Configuration
   */
  @NonNull
  private final Configuration conf;
  @NonNull
  private final HTable table;

  public void writeHFiles(@NonNull JavaRDD<ObjectNode> input, @NonNull Path hFilePath) {
    val processed = process(input);
    writeOutput(hFilePath, processed);
  }

  @SneakyThrows
  private JavaPairRDD<ImmutableBytesWritable, KeyValue> process(JavaRDD<ObjectNode> input) {
    return input
        .mapToPair(new TranslateHBaseKeyValue())
        .partitionBy(new HFilePartitioner(conf, table.getStartKeys()));
  }

  private void writeOutput(Path hFilePath, JavaPairRDD<ImmutableBytesWritable, KeyValue> processed) {
    processed.saveAsNewAPIHadoopFile(
        hFilePath.toString(),
        ImmutableBytesWritable.class, KeyValue.class,
        HFileOutputFormat2.class, conf);
  }

}
