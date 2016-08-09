/*
 * Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.release.job.export.task;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.icgc.dcc.common.core.util.Joiners.TAB;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;
import static org.icgc.dcc.common.hadoop.util.HadoopCompression.GZIP;
import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.icgc.dcc.common.core.model.DownloadDataType;
import org.icgc.dcc.common.core.util.Joiners;
import org.icgc.dcc.common.core.util.Separators;
import org.icgc.dcc.common.hadoop.fs.HadoopUtils;
import org.icgc.dcc.release.core.task.GenericTask;
import org.icgc.dcc.release.core.task.TaskContext;
import org.icgc.dcc.release.core.task.TaskType;

@Slf4j
@RequiredArgsConstructor
public class WriteHeadersTask extends GenericTask {

  public static final String HEADERS_DIR_PATH = "headers";

  @NonNull
  private final String exportDir;

  @Override
  public TaskType getType() {
    return TaskType.FILE_TYPE;
  }

  @Override
  public void execute(TaskContext taskContext) {
    log.info("Executing '{}'...", this.getClass().getName());
    val fileSystem = taskContext.getFileSystem();
    val headersPath = resolveHeadersDirPath(taskContext.getJobContext().getWorkingDir());
    log.info("DownloadDataType headers path: {}", headersPath);

    HadoopUtils.mkdirs(fileSystem, headersPath);
    val codec = getCompressionCodec(fileSystem);
    stream(DownloadDataType.values())
        .forEach(dt -> writeHeader(dt, fileSystem, headersPath, codec));
  }

  @SneakyThrows
  private static void writeHeader(DownloadDataType dataType, FileSystem fileSystem, Path headersPath,
      CompressionCodec codec) {
    val header = getHeader(dataType);
    val headerFile = resolveHeaderFile(headersPath, dataType);
    log.debug("Saving {} header to {}", dataType, headerFile);

    @Cleanup
    val out = codec.createOutputStream(fileSystem.create(headerFile));
    out.write(header.getBytes(UTF_8));
  }

  private static String getHeader(DownloadDataType dataType) {
    val columns = dataType.getFields().entrySet().stream()
        .map(e -> e.getValue())
        .collect(toImmutableList());

    val header = TAB.join(columns);

    return header + Separators.NEWLINE;
  }

  private static Path resolveHeaderFile(Path headersPath, DownloadDataType dataType) {
    return new Path(headersPath, dataType.getId() + ".tsv.gz");
  }

  private Path resolveHeadersDirPath(String workingDir) {
    val headersDirPath = Joiners.PATH.join(workingDir, exportDir, HEADERS_DIR_PATH);

    return new Path(headersDirPath);
  }

  private static CompressionCodec getCompressionCodec(FileSystem fileSystem) {
    val compressionFactory = new CompressionCodecFactory(fileSystem.getConf());

    return compressionFactory.getCodecByName(GZIP.getCodec());
  }

}
