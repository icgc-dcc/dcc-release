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
package org.icgc.dcc.release.job.index.task;

import static com.google.common.base.Preconditions.checkState;
import static java.util.regex.Pattern.compile;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.checkExistence;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.getInputStream;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.lsFile;
import static org.icgc.dcc.release.core.document.DocumentType.DONOR_CENTRIC_TYPE;
import static org.icgc.dcc.release.core.util.JacksonFactory.SMILE_READER;
import static org.icgc.dcc.release.job.index.io.DocumentWriterFactory.createDocumentWriter;
import static org.icgc.dcc.release.job.index.utils.IndexTasks.getBigFilesPath;
import static org.icgc.dcc.release.job.index.utils.IndexTasks.getDocumentTypeFromFileName;
import static org.icgc.dcc.release.job.index.utils.IndexTasks.getIdFromFileName;
import static org.icgc.dcc.release.job.index.utils.IndexTasks.getIndexName;

import java.util.Collection;
import java.util.Collections;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.release.core.document.Document;
import org.icgc.dcc.release.core.task.GenericTask;
import org.icgc.dcc.release.core.task.TaskContext;
import org.icgc.dcc.release.core.task.TaskType;
import org.icgc.dcc.release.job.index.utils.IndexTasks;

import com.fasterxml.jackson.databind.node.ObjectNode;

@Slf4j
@RequiredArgsConstructor
public class IndexBigFilesTask extends GenericTask {

  @NonNull
  private final String esUri;

  @Override
  @SneakyThrows
  public void execute(TaskContext taskContext) {
    val files = getFiles(taskContext);
    if (files.isEmpty()) {
      log.info("No big files found for indexing.");
      return;
    }

    val indexName = getIndexName(taskContext.getJobContext().getReleaseName());
    @Cleanup
    val documentWriter = createDocumentWriter(esUri, indexName, DONOR_CENTRIC_TYPE);

    int loadedDocsCount = 0;
    for (val file : files) {
      val document = createDocument(taskContext.getFileSystem(), file);
      log.info("[{}/{}] Loading document {} to {} index type...", ++loadedDocsCount, files.size(), document.getId(),
          document.getType().getName());
      documentWriter.write(document);
    }
  }

  private static Document createDocument(FileSystem fileSystem, Path path) {
    val fileName = path.getName();
    val type = getDocumentTypeFromFileName(fileName);
    val id = getIdFromFileName(fileName);
    val source = readSource(fileSystem, path);
    checkState(source.isObject(), "Source is not a valid ObjectNode");

    return new Document(type, id, source);
  }

  @SneakyThrows
  private static ObjectNode readSource(FileSystem fileSystem, Path path) {
    @Cleanup
    val inputStream = getInputStream(fileSystem, path);

    return SMILE_READER.readValue(inputStream);
  }

  private Collection<Path> getFiles(TaskContext taskContext) {
    val bigFilesPath = getBigFilesPath(taskContext.getJobContext().getWorkingDir());
    log.debug("Checking path {} for big files...", bigFilesPath);

    val fileSystem = taskContext.getFileSystem();
    if (!checkExistence(fileSystem, bigFilesPath)) {
      log.debug("{} path does not exist.", bigFilesPath);

      return Collections.emptyList();
    }

    return lsFile(fileSystem, bigFilesPath, compile(".*" + IndexTasks.GZIP_EXTENSION + "$"));
  }

  @Override
  public TaskType getType() {
    return TaskType.FILE_TYPE;
  }

}
