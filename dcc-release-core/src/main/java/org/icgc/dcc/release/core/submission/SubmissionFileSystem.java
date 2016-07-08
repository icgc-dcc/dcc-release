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
package org.icgc.dcc.release.core.submission;

import static com.google.common.base.Stopwatch.createStarted;
import static com.google.common.collect.Lists.newArrayList;

import java.util.List;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;

/**
 * Service for interacting with the DCC submission file system.
 */
@Slf4j
@Lazy
@Service
@RequiredArgsConstructor(onConstructor = @__({ @Autowired }))
public class SubmissionFileSystem {

  /**
   * Dependencies.
   */
  @NonNull
  private final FileSystem fileSystem;

  @NonNull
  @SneakyThrows
  public Table<String, String, List<Path>> getFiles(String releaseDir, List<String> projectNames,
      List<SubmissionFileSchema> metadata) {
    val watch = createStarted();
    log.info("Resolving submission files...");

    val table = TreeBasedTable.<String, String, List<Path>> create();
    val iterator = fileSystem.listFiles(new Path(releaseDir), true);

    while (iterator.hasNext()) {
      val status = iterator.next();
      val path = status.getPath();

      for (val schema : metadata) {
        val name = path.getName();
        if (name.matches(schema.getPattern())) {
          addFile(projectNames, schema, path, table);
        }
      }
    }

    log.info("Finished resolving submission files in {}", watch);
    return table;
  }

  private void addFile(List<String> projectNames, SubmissionFileSchema schema, Path path,
      Table<String, String, List<Path>> files) {
    val schemaName = schema.getName();
    val projectName = path.getParent().getName();
    if (isTestProject(projectName)) {
      // Skip test projects
      return;
    }

    if (!projectNames.contains(projectName)) {
      // Skip unspecified projects
      return;
    }

    List<Path> paths = files.get(schemaName, projectName);
    if (paths == null) {
      paths = newArrayList();
      files.put(schemaName, projectName, paths);
    }

    paths.add(path);
  }

  private static boolean isTestProject(String projectName) {
    return projectName.startsWith("TEST");
  }

}
