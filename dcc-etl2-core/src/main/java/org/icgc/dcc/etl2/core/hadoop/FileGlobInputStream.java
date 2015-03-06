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
package org.icgc.dcc.etl2.core.hadoop;

import static java.util.Collections.enumeration;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Collection;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.icgc.dcc.common.core.io.ForwardingInputStream;

import com.google.common.collect.Lists;

/**
 * Inspired from how {@code FileInputFormat} resolves its input files.
 */
@Slf4j
public class FileGlobInputStream extends ForwardingInputStream {

  /**
   * Copied from {@code FileInputFormat}
   */
  private static PathFilter HIDDEN_PATH_FILTER = new PathFilter() {

    @Override
    public boolean accept(Path path) {
      val name = path.getName();

      return !name.startsWith("_") && !name.startsWith(".");
    }

  };

  public FileGlobInputStream(@NonNull FileSystem fileSystem, @NonNull Path pathPattern) {
    super(createInputStream(fileSystem, pathPattern), true);
  }

  private static InputStream createInputStream(FileSystem fileSystem, Path pathPattern) {
    val inputStreams = Lists.<InputStream> newArrayList();

    try {
      val factory = new CompressionCodecFactory(fileSystem.getConf());

      val paths = getPaths(fileSystem, pathPattern);
      for (val path : paths) {
        log.info("Creating input stream for '{}'", path);
        val inputStream = createDecodedInputStream(fileSystem, path, factory);

        inputStreams.add(inputStream);
      }
    } catch (IOException e) {
      throw new RuntimeException("Error reading: '" + pathPattern.toString() + "'", e);
    }

    return combineInputStreams(inputStreams);
  }

  private static Collection<Path> getPaths(FileSystem fileSystem, Path pathPattern) throws IOException,
      FileNotFoundException {

    FileStatus[] matches = fileSystem.globStatus(pathPattern, HIDDEN_PATH_FILTER);
    val paths = Lists.<Path> newArrayList();
    for (val match : matches) {
      if (fileSystem.isDirectory(match.getPath())) {
        FileStatus[] partFiles = fileSystem.listStatus(match.getPath(), HIDDEN_PATH_FILTER);
        for (val partFile : partFiles) {
          paths.add(partFile.getPath());
        }
      } else {
        paths.add(match.getPath());
      }
    }

    return paths;
  }

  private static SequenceInputStream combineInputStreams(Collection<InputStream> inputStreams) {
    // Combine the input streams into a chain
    return new SequenceInputStream(enumeration(inputStreams));
  }

  private static InputStream createDecodedInputStream(FileSystem fileSystem, Path file, CompressionCodecFactory factory)
      throws IOException {
    val codec = factory.getCodec(file);
    val decoded = codec == null;

    return decoded ? fileSystem.open(file) : codec.createInputStream(fileSystem.open(file));
  }

}
