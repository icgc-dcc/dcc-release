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

import java.io.FileNotFoundException;
import java.io.IOException;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.icgc.dcc.etl2.job.export.function.TranslateHBaseKeyValue;

import com.fasterxml.jackson.databind.node.ObjectNode;

@RequiredArgsConstructor
public class HFileManager {

  /**
   * Constants.
   */
  private static final Path HFILE_DIR_PATH = new Path("/tmp/exporter/tmp/hfile");
  private static final FsPermission rwx = new FsPermission("777");

  /**
   * Configuration
   */
  @NonNull
  private final Configuration conf;
  @NonNull
  private final FileSystem fileSystem;

  public void writeHFiles(@NonNull JavaRDD<ObjectNode> input, @NonNull HTable htable) {
    val hFilesPath = getHFilesPath(fileSystem, htable);
    val processed = process(input, htable);
    writeOutput(hFilesPath, processed);
  }

  @SneakyThrows
  private JavaPairRDD<ImmutableBytesWritable, KeyValue> process(@NonNull JavaRDD<ObjectNode> input,
      @NonNull HTable htable) {

    // JavaPairRDD<ObjectNode, Long> a = input.zipWithIndex();
    // val b = a.mapToPair(new TranslateHBaseKeyValue3());
    // val c = b.sortByKey(true).coalesce(10, true);
    // val groupedDonors = baseExportProcessResult.mapToPair(new GroupByDonorId());
    // val donors = groupedDonors.keys();

    return input
        .mapToPair(new TranslateHBaseKeyValue())
        .partitionBy(new HFilePartitioner(conf, htable.getStartKeys()));
  }

  private void writeOutput(Path hFilePath, JavaPairRDD<ImmutableBytesWritable, KeyValue> processed) {
    processed.saveAsNewAPIHadoopFile(
        hFilePath.toString(),
        ImmutableBytesWritable.class, KeyValue.class,
        HFileOutputFormat2.class, conf);
  }

  private static Path getHFilesPath(FileSystem fileSystem, HTable hTable) {
    val htableName = new String(hTable.getTableName());
    val hFilePath = new Path(HFILE_DIR_PATH, htableName);

    return fileSystem.makeQualified(hFilePath);
  }

  @SneakyThrows
  public void loadHFiles(@NonNull HTable htable) {
    val hFilesPath = getHFilesPath(fileSystem, htable);
    prepare(hFilesPath);

    load(htable, hFilesPath);
  }

  private void prepare(Path hFilePath) throws FileNotFoundException, IOException {
    // Prepare HFiles for incremental load
    FileStatus[] hFiles = fileSystem.listStatus(hFilePath);

    for (val hFile : hFiles) {
      // Set folders permissions read/write/exec for all
      fileSystem.setPermission(hFile.getPath(), rwx);

      if (hFile.isDirectory()) {
        val splitDir = getHFileSplitDir(hFile.getPath());

        fileSystem.mkdirs(splitDir, rwx);
      }
    }
  }

  private void load(HTable table, Path hFilePath) throws Exception, TableNotFoundException, IOException {
    val loader = new LoadIncrementalHFiles(conf);
    loader.doBulkLoad(hFilePath, table);
  }

  private static Path getHFileSplitDir(Path hFile) {
    // Create a "_tmp" folder that can be used for HFile splitting, so that we can
    // set permissions correctly. This is a workaround for unsecured HBase. It should not
    // be necessary for SecureBulkLoadEndpoint (see https://issues.apache.org/jira/browse/HBASE-8495
    // and http://comments.gmane.org/gmane.comp.java.hadoop.hbase.user/44273)
    val splitDir = new Path(hFile, "_tmp");

    return splitDir;
  }

}
