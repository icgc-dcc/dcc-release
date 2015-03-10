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
package org.icgc.dcc.etl2.job.stage.function;

import static com.google.common.base.Preconditions.checkState;

import java.util.Iterator;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.spark.api.java.function.Function2;
import org.icgc.dcc.etl2.core.submission.SubmissionFileSchema;
import org.icgc.dcc.etl2.core.submission.SubmissionFiles;
import org.icgc.dcc.etl2.job.stage.util.ParseFileSplitIterator;

import scala.Tuple2;

import com.fasterxml.jackson.databind.node.ObjectNode;

@RequiredArgsConstructor
public class ParseFileSplit implements
    Function2<InputSplit, Iterator<Tuple2<LongWritable, Text>>, Iterator<ObjectNode>> {

  /**
   * Metadata.
   */
  @NonNull
  private final SubmissionFileSchema schema;

  @Override
  public Iterator<ObjectNode> call(InputSplit split, Iterator<Tuple2<LongWritable, Text>> iterator) throws Exception {
    val projectFilePath = getFilePath(split);
    val projectPath = SubmissionFiles.getProjectPath(projectFilePath);
    val projectName = SubmissionFiles.getProjectName(projectPath);
    val projectSplitName = split.toString();
    val projectFileFields = schema.getFields();

    // Lazy iterator
    return new ParseFileSplitIterator(iterator, projectFileFields, projectSplitName, split.getLength(), projectName);
  }

  public static Path getFilePath(InputSplit split) {
    if (split instanceof FileSplit) {
      val fileSplit = (FileSplit) split;
      return fileSplit.getPath();
    }

    if (split instanceof CombineFileSplit) {
      val combineFileSplit = (CombineFileSplit) split;
      return combineFileSplit.getPath(0);
    }

    checkState(false, "Unsupported input split class: %s split = '%s'", split.getClass(), split);

    return null;
  }

}