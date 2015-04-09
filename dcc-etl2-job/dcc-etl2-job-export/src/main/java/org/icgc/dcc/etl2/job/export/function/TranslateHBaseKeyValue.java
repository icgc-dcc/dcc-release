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
package org.icgc.dcc.etl2.job.export.function;

import static org.icgc.dcc.etl2.core.util.ObjectNodes.textValue;
import static org.icgc.dcc.etl2.job.export.model.Constants.DONOR_ID;
import lombok.val;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.function.PairFunction;
import org.icgc.dcc.etl2.job.export.model.ExportTables;

import scala.Tuple2;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class TranslateHBaseKeyValue implements PairFunction<ObjectNode, ImmutableBytesWritable, KeyValue> {

  @Override
  public Tuple2<ImmutableBytesWritable, KeyValue> call(ObjectNode record) {
    val key = createKey(record);
    byte[] row = key.get();
    byte[] value = createValue(record);

    val keyValue = createKeyValue(row, value);

    return createTuple(key, keyValue);
  }

  private byte[] createValue(ObjectNode record) {
    return Bytes.toBytes(record.toString());
  }

  private ImmutableBytesWritable createKey(ObjectNode record) {
    // TODO: Make configurable
    val value = textValue(record, DONOR_ID);
    val key = new ImmutableBytesWritable();
    key.set(Bytes.toBytes(value));

    return key;
  }

  private KeyValue createKeyValue(byte[] row, byte[] value) {
    // TODO: Make configurable
    byte[] family = ExportTables.META_TYPE_INFO_FAMILY;
    byte[] qualifier = ExportTables.META_TYPE_HEADER;

    return new KeyValue(row, family, qualifier, value);
  }

  private Tuple2<ImmutableBytesWritable, KeyValue> createTuple(ImmutableBytesWritable key, KeyValue keyValue) {
    return new Tuple2<ImmutableBytesWritable, KeyValue>(key, keyValue);
  }

}