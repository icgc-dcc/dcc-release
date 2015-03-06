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

import java.io.IOException;

import lombok.val;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileRecordReader;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

public class CombineTextInputFormat extends CombineFileInputFormat<LongWritable, Text> {

  @Override
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public RecordReader<LongWritable, Text> getRecordReader(InputSplit split, JobConf conf, Reporter reporter)
      throws IOException {
    return new CombineFileRecordReader(conf, (CombineFileSplit) split, reporter, TextRecordReaderWrapper.class);
  }

  @Override
  protected boolean isSplitable(FileSystem fs, Path filename) {
    // Default is true. Make sure file will not be split by returning false.
    val splitable = false;

    return splitable;
  }

  private static abstract class CombineFileRecordReaderWrapper<K, V> implements RecordReader<K, V> {

    private final RecordReader<K, V> delegate;

    protected CombineFileRecordReaderWrapper(FileInputFormat<K, V> inputFormat, CombineFileSplit split,
        Configuration conf, Reporter reporter, Integer index) throws IOException {
      val fileSplit = new FileSplit(
          split.getPath(index),
          split.getOffset(index),
          split.getLength(index),
          split.getLocations());

      delegate = inputFormat.getRecordReader(fileSplit, (JobConf) conf, reporter);
    }

    @Override
    public boolean next(K key, V value) throws IOException {
      return delegate.next(key, value);
    }

    @Override
    public K createKey() {
      return delegate.createKey();
    }

    @Override
    public V createValue() {
      return delegate.createValue();
    }

    @Override
    public long getPos() throws IOException {
      return delegate.getPos();
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }

    @Override
    public float getProgress() throws IOException {
      return delegate.getProgress();
    }

  }

  public static class TextRecordReaderWrapper extends CombineFileRecordReaderWrapper<LongWritable, Text> {

    public TextRecordReaderWrapper(CombineFileSplit split, Configuration conf, Reporter reporter, Integer index)
        throws IOException {
      super(new TextInputFormat(), split, conf, reporter, index);
    }

  }

}