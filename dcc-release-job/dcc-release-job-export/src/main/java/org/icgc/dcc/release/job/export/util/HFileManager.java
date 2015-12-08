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
package org.icgc.dcc.release.job.export.util;

import static org.icgc.dcc.release.core.util.HadoopFileSystemUtils.getFilePaths;
import static org.icgc.dcc.release.job.export.model.ExportTables.BLOCKSIZE;
import static org.icgc.dcc.release.job.export.model.ExportTables.DATA_CONTENT_FAMILY;
import static org.icgc.dcc.release.job.export.model.ExportTables.TMP_HFILE_ROOT;
import static org.icgc.dcc.release.job.export.model.ExportTables.RWX;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Map.Entry;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFile.Writer;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;
import scala.Tuple3;

@Slf4j
@RequiredArgsConstructor
public class HFileManager {

  /**
   * Configuration
   */
  @NonNull
  private final Configuration conf;
  @NonNull
  private final FileSystem fileSystem;

  public void writeHFiles(
      @NonNull JavaPairRDD<String, Tuple3<Map<ByteBuffer, KeyValue[]>, Long, Integer>> processedInput,
      @NonNull HTable hTable)
      throws IOException {
    val hFilesPath = getHFilesPath(fileSystem, hTable);
    val fsUri = fileSystem.getUri().toString();
    processedInput.foreach(new HFileWriter(hFilesPath.toString(), fsUri));

    // TODO: Remove verification
    val files = getFilePaths(fileSystem, hFilesPath);
    files.forEach(log::info);
  }

  private static Path getHFilesPath(FileSystem fileSystem, HTable hTable) {
    val htableName = Bytes.toString(hTable.getTableName());
    val hFilePath = new Path(TMP_HFILE_ROOT, htableName);

    return fileSystem.makeQualified(hFilePath);
  }

  @SneakyThrows
  public void loadHFiles(@NonNull HTable htable) {
    val hFilesDirPath = getHFilesPath(fileSystem, htable);
    prepare(hFilesDirPath);

    log.info("Creating load job...");
    val hFileLoadJob = createHFileLoadJob(conf, htable);
    log.info("Created load job: {}", hFileLoadJob);

    load(htable, hFilesDirPath);
  }

  @SneakyThrows
  private void prepare(Path hFilePath) {
    // Prepare HFiles for incremental load
    FileStatus[] hFiles = fileSystem.listStatus(hFilePath);

    for (val hFile : hFiles) {
      // Set folders permissions read/write/exec for all
      fileSystem.setPermission(hFile.getPath(), RWX);

      if (hFile.isDirectory()) {
        val splitDir = getHFileSplitDir(hFile.getPath());

        fileSystem.mkdirs(splitDir, RWX);
      }
    }
  }

  @SneakyThrows
  private void load(HTable table, Path hFilesDirPath) {
    val loader = new LoadIncrementalHFiles(conf);
    loader.doBulkLoad(hFilesDirPath, table);
  }

  private static Path getHFileSplitDir(Path hFile) {
    // Create a "_tmp" folder that can be used for HFile splitting, so that we can
    // set permissions correctly. This is a workaround for unsecured HBase. It should not
    // be necessary for SecureBulkLoadEndpoint (see https://issues.apache.org/jira/browse/HBASE-8495
    // and http://comments.gmane.org/gmane.comp.java.hadoop.hbase.user/44273)

    return new Path(hFile, "_tmp");
  }

  private Job createHFileLoadJob(Configuration conf, HTable table) {
    val factory = new HFileLoadJobFactory(conf);

    return factory.createJob(table);
  }

  /**
   * See:
   *
   * <pre>
   * https://github.com/icgc-dcc/dcc-etl/blob/develop/dcc-etl-exporter/src/main/java/org/icgc/dcc/etl/exporter/pig/udf/ToHFile.java#L94
   * </pre>
   */
  @RequiredArgsConstructor
  private static class HFileWriter implements
      VoidFunction<Tuple2<String, Tuple3<Map<ByteBuffer, KeyValue[]>, Long, Integer>>> {

    @NonNull
    private final String hfilesPath;

    @NonNull
    private final String fsUri;

    @Override
    @SneakyThrows
    public void call(Tuple2<String, Tuple3<Map<ByteBuffer, KeyValue[]>, Long, Integer>> tuple) {
      val donorId = tuple._1();
      val data = tuple._2()._1();
      writeOutput(donorId, data);
    }

    @SneakyThrows
    private Writer createWriter(String donorId) throws IOException {
      Path destPath = new Path(hfilesPath, Bytes.toString(DATA_CONTENT_FAMILY));
      Configuration conf = new Configuration();
      FileSystem fileSystem = FileSystem.get(new URI(fsUri), new Configuration());

      return HFile
          .getWriterFactory(conf, new CacheConfig(conf))
          .withPath(fileSystem, new Path(destPath, donorId))
          .withComparator(KeyValue.COMPARATOR)
          .withFileContext(
              new HFileContextBuilder().withBlockSize(BLOCKSIZE)
                  .build()).create();
    }

    private void closeWriter(Writer writer) {
      if (writer != null) {
        try {
          writer.close();
        } catch (IOException e) {
          log.error("Fail to close the HFile writer", e);
        }
      }
    }

    @SneakyThrows
    private void writeOutput(String donorId, Map<ByteBuffer, KeyValue[]> processed) {
      Writer writer = createWriter(donorId);
      for (Entry<ByteBuffer, KeyValue[]> processedEntry : processed.entrySet()) {
        KeyValue[] cells = processedEntry.getValue();
        for (val cell : cells) {
          writer.append(cell);
        }
      }
      closeWriter(writer);
    }
  }

}
