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
package org.icgc.dcc.release.job.export.util;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.Partitioner;

@RequiredArgsConstructor
public class HFilePartitioner extends Partitioner {

  /**
   * Constants.
   * <p>
   * TODO: Not available in this version of HBase. Fix after upgrade.
   */
  private static final String LoadIncrementalHFiles_MAX_FILES_PER_REGION_PER_FAMILY =
      "hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily";

  /**
   * Configuration.
   */
  private final int regionFileCount;
  @NonNull
  private final byte[][] splitKeys;

  public HFilePartitioner(@NonNull Configuration conf, @NonNull byte[][] splitKeys) {
    this.regionFileCount = getRegionFileCount(conf);
    this.splitKeys = splitKeys;
  }

  @Override
  public int getPartition(Object rawKey) {
    val key = toBytes(rawKey);
    val offset = getHash(key);
    val n = splitKeys.length;

    // Iterate in order to find the least upper bound (LUB)
    for (int splitNumber = 1; splitNumber <= n; splitNumber++) {
      if (isKeyBeforeSplit(key, splitNumber)) {
        return translatePartition(offset, splitNumber);
      }
    }

    return translatePartition(offset, n);
  }

  @Override
  public int numPartitions() {
    return splitKeys.length * regionFileCount;
  }

  private int translatePartition(int offset, int splitNumber) {
    return (splitNumber - 1) * regionFileCount + offset;
  }

  private boolean isKeyBeforeSplit(byte[] key, int splitNumber) {
    return Bytes.compareTo(key, splitKeys[splitNumber - 1]) < 0;
  }

  private int getHash(byte[] key) {
    val positiveHashCode = System.identityHashCode(key) & 0x7fffffff;

    return positiveHashCode % regionFileCount;
  }

  private static byte[] toBytes(Object object) {
    if (object instanceof Boolean) {
      return Bytes.toBytes((Boolean) object);
    }
    if (object instanceof Double) {
      return Bytes.toBytes((Double) object);
    }
    if (object instanceof Float) {
      return Bytes.toBytes((Float) object);
    }
    if (object instanceof Integer) {
      return Bytes.toBytes((Integer) object);
    }
    if (object instanceof Long) {
      return Bytes.toBytes((Long) object);
    }
    if (object instanceof Short) {
      return Bytes.toBytes((Short) object);
    }
    if (object instanceof String) {
      return Bytes.toBytes((String) object);
    }
    if (object instanceof ImmutableBytesWritable) {
      return ((ImmutableBytesWritable) object).get();
    }
    if (object instanceof byte[]) {
      return (byte[]) object;
    }

    throw new IllegalArgumentException("Cannot convert " + object + " of type " + object.getClass() + "  to byte[]");
  }

  private static int getRegionFileCount(Configuration conf) {
    return conf.getInt(LoadIncrementalHFiles_MAX_FILES_PER_REGION_PER_FAMILY, 32);
  }

}