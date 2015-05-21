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

import static org.icgc.dcc.etl2.job.export.model.ExportTables.DATA_CONTENT_FAMILY;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFile.Writer;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.icgc.dcc.etl2.job.export.function.ExtractKeyValues;

import scala.Tuple3;

@Slf4j
@RequiredArgsConstructor
public class HFileManager {

  /**
   * Constants.
   */
  private static final Path HFILE_DIR_PATH = new Path("/tmp/exporter/tmp/hfile");
  private static final FsPermission rwx = new FsPermission("777");
  private static int BLOCKSIZE = 5 * 1048576;
  public static Algorithm COMPRESSION = Compression.Algorithm.SNAPPY;

  /**
   * Configuration
   */
  @NonNull
  private final Configuration conf;
  @NonNull
  private final FileSystem fileSystem;

  public void writeHFiles(@NonNull JavaPairRDD<String, Tuple3<KeyValue[], Long, Integer>> input, @NonNull HTable htable)
      throws IOException {
    val hFilesPath = getHFilesPath(fileSystem, htable);
    List<KeyValue[]> keyValues = input.map(new ExtractKeyValues()).collect();
    writeOutput(hFilesPath, keyValues);
  }

  private void writeOutput(Path hFilePath, List<KeyValue[]> processed) throws IOException {
    Writer writer = createWriter(hFilePath.toString());
    for (KeyValue[] donorKeyValues : processed) {
      for (KeyValue kv : donorKeyValues) {
        writer.append(kv);
      }
    }
    writer.close();
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

    log.info("Creating load job...");
    val hFileLoadJob = createHFileLoadJob(conf, htable);
    log.info("Created load job: {}", hFileLoadJob);

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

  private Job createHFileLoadJob(Configuration conf, HTable table) {
    val factory = new HFileLoadJobFactory(conf);

    return factory.createJob(table);
  }

  private Writer createWriter(String donorId) throws IOException {
    Path destPath = new Path(HFILE_DIR_PATH, Bytes.toString(DATA_CONTENT_FAMILY));
    val writer = HFile
        .getWriterFactory(conf, new CacheConfig(conf))
        .withPath(fileSystem, new Path(destPath, donorId))
        .withComparator(KeyValue.COMPARATOR)
        .withFileContext(
            new HFileContextBuilder().withBlockSize(BLOCKSIZE)
                .withCompression(COMPRESSION).build()).create();

    return writer;
  }

}
