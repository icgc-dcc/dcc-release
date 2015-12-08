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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import com.google.common.collect.Lists;

@Slf4j
public class HadoopFileSystemUtils {

  @SneakyThrows
  public static void viewFileContents(FileSystem fileSystem, Path inputPath) {
    val files = getFiles(fileSystem, inputPath, true);
    for (val file : files) {
      val contents = readFile(fileSystem, file.getPath());
      for (val line : contents) {
        log.info(line);
      }
    }
  }

  @SneakyThrows
  public static List<String> getFilePaths(FileSystem fileSystem, Path inputPath) {
    val results = Lists.<String> newArrayList();
    val files = getFiles(fileSystem, inputPath, true);
    for (val file : files) {
      results.add(file.getPath().toString());
    }

    return results;
  }

  private static List<LocatedFileStatus> getFiles(FileSystem fileSystem, Path target, boolean recusre) {
    val results = Lists.<LocatedFileStatus> newArrayList();
    RemoteIterator<LocatedFileStatus> fileStatusListIterator = null;
    try {
      fileStatusListIterator = fileSystem.listFiles(target, true);
      while (fileStatusListIterator.hasNext()) {
        LocatedFileStatus fileStatus = fileStatusListIterator.next();
        results.add(fileStatus);
      }
    } catch (IOException e) {
      log.info("Error retriving files in path '{}'", target);
    }
    return results;
  }

  @SneakyThrows
  public static List<String> readFile(FileSystem fileSystem, Path inputPath) {
    val results = Lists.<String> newArrayList();
    @Cleanup
    val br = new BufferedReader(new InputStreamReader(fileSystem.open(inputPath), UTF_8));
    String line;
    line = br.readLine();
    while (line != null) {
      line = br.readLine();
      results.add(line);
    }
    return results;
  }

}
