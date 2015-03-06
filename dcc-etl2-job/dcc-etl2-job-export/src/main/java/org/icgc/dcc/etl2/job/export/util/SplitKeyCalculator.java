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

import java.io.IOException;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.icgc.dcc.etl2.job.export.function.CoercePair;
import org.icgc.dcc.etl2.job.export.function.PairIteratorFirstKey;

@RequiredArgsConstructor
public class SplitKeyCalculator {

  /**
   * Configuration
   */
  @NonNull
  private final Configuration conf;

  @SneakyThrows
  public Iterable<String> calculateSplitKeys(@NonNull Path inputPath, @NonNull JavaRDD<String> keys, long regionSize) {
    val fileSystem = inputPath.getFileSystem(conf);
    val fileLength = getFileLength(inputPath, fileSystem);
    val splitsCount = calculateSplitCount(regionSize, fileLength);

    return calculateSplitKeys(keys, splitsCount);
  }

  private Iterable<String> calculateSplitKeys(@NonNull JavaRDD<String> keys, int splitsCount) {
    val ascending = true;

    return keys
        .mapToPair(new CoercePair<String>())
        .sortByKey(ascending, splitsCount)
        .mapPartitions(new PairIteratorFirstKey<String, Void>())
        .collect();
  }

  private static long getFileLength(Path inputPath, FileSystem fileSystem) throws IOException {
    val summary = fileSystem.getContentSummary(inputPath);

    return summary.getLength();
  }

  private static int calculateSplitCount(long regionSize, final long fileLength) {
    return (int) (fileLength / regionSize) + 1;
  }

}
