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

import static com.google.common.collect.Iterables.toArray;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.common.core.util.FormatUtils.formatPercent;
import static org.icgc.dcc.common.core.util.Splitters.TAB;
import static org.icgc.dcc.etl2.core.function.ParseObjectNode.MAPPER;
import static org.icgc.dcc.etl2.core.util.Stopwatches.createStarted;

import java.util.Iterator;
import java.util.List;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.spark.api.java.function.Function2;
import org.icgc.dcc.common.core.model.FieldNames;
import org.icgc.dcc.etl2.core.submission.Field;
import org.icgc.dcc.etl2.core.submission.Schema;

import scala.Tuple2;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Stopwatch;

@Slf4j
@RequiredArgsConstructor
public class ParseLine implements Function2<InputSplit, Iterator<Tuple2<LongWritable, Text>>, Iterator<ObjectNode>> {

  /**
   * Metadata.
   */
  @NonNull
  private final Schema schema;

  @Override
  public Iterator<ObjectNode> call(InputSplit split, Iterator<Tuple2<LongWritable, Text>> iterator) throws Exception {
    val fileSplit = (FileSplit) split;
    val projectName = getProjectName(fileSplit);

    // Lazy iterator
    return new ParseLineIterator(iterator, schema.getFields(), fileSplit.toString(), fileSplit.getLength(),
        projectName);
  }

  private static String getProjectName(FileSplit fileSplit) {
    return fileSplit.getPath().getParent().getName();
  }

  private static boolean isHeader(Tuple2<LongWritable, Text> record) {
    return getOffset(record) == 0;
  }

  private static long getOffset(Tuple2<LongWritable, Text> record) {
    return record._1.get();
  }

  private static String getLine(Tuple2<LongWritable, Text> record) {
    return record._2.toString();
  }

  private static String[] parseLine(String line) {
    return toArray(TAB.split(line), String.class);
  }

  @RequiredArgsConstructor
  private static class ParseLineIterator implements Iterator<ObjectNode> {

    /**
     * Constants.
     */
    private static final int LINE_STATUS_COUNT = 10 * 1000 * 1000;

    /**
     * Dependencies.
     */
    private final Iterator<Tuple2<LongWritable, Text>> delegate;

    /**
     * Metadata
     */
    private final List<Field> fields;
    private final String fileSplitName;
    private final long fileSplitLength;
    private final String projectName;

    /**
     * State.
     */
    private final Stopwatch watch = createStarted();
    private long lineCount = 0;
    private Tuple2<LongWritable, Text> record;

    @Override
    public boolean hasNext() {
      if (!delegate.hasNext()) {
        // Finished
        return false;
      }

      // Peek
      record = delegate.next();

      updateProgress();

      // Remove header
      if (isHeader(record)) {
        // Maybe more, lets see...
        return hasNext();
      }

      // More
      return true;
    }

    @Override
    public ObjectNode next() {
      val line = getLine(record);
      String[] values = parseLine(line);

      return createRow(values);
    }

    private ObjectNode createRow(String[] values) {
      val objectNode = MAPPER.createObjectNode();
      for (int i = 0; i < fields.size(); i++) {
        val fieldName = fields.get(i).getName();
        val fieldValue = values[i];

        objectNode.put(fieldName, fieldValue);
      }

      objectNode.put(FieldNames.PROJECT_ID, projectName);
      return objectNode;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Cannot remove a " + getClass().getName() + " iterator");
    }

    private void updateProgress() {
      if (lineCount++ % LINE_STATUS_COUNT == 0) {
        val offset = getOffset(record);
        val percent = offset * 1.0 / fileSplitLength;

        log.info("{}: Processed {} lines ({} %) in {}",
            fileSplitName, formatCount(lineCount), formatPercent(percent), watch);
      }
    }

  }

}