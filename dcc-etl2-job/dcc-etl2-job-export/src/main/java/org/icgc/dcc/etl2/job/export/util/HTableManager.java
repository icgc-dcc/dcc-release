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

import static org.icgc.dcc.etl2.job.export.model.ExportTables.META_BLOCK_SIZE;
import static org.icgc.dcc.etl2.job.export.model.ExportTables.META_SIZE_INFO_FAMILY;
import static org.icgc.dcc.etl2.job.export.model.ExportTables.META_TYPE_INFO_FAMILY;
import static org.icgc.dcc.etl2.job.export.model.ExportTables.NUM_REGIONS;

import java.util.List;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.icgc.dcc.etl2.job.export.model.CompositeRowKey;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeMap;
import com.google.common.primitives.UnsignedBytes;

/**
 * See:
 * 
 * <pre>
 * https://github.com/icgc-dcc/dcc-downloader/blob/develop/dcc-downloader-core/src/main/java/org/icgc/dcc/downloader/core/SchemaUtil.java
 * </pre>
 */
@Slf4j
@RequiredArgsConstructor
public class HTableManager {

  /**
   * Dependencies.
   */
  @NonNull
  private final HBaseAdmin admin;

  @SneakyThrows
  public HTable ensureTable(@NonNull String tableName, @NonNull List<byte[]> splitKeys) {
    val withSnappyCompression = false;

    if (!admin.tableExists(tableName)) {
      val compressionType = withSnappyCompression ? Algorithm.SNAPPY : Algorithm.NONE;
      val descriptor = createTableDescriptor(tableName, compressionType);

      admin.createTable(descriptor);
    }

    return new HTable(admin.getConfiguration(), tableName);
  }

  @SneakyThrows
  public void listTables() {
    for (val table : admin.listTables()) {
      log.info("{}", table);
    }
  }

  private HTableDescriptor createTableDescriptor(String tableName, Algorithm compressionType) {
    val typeColumnFamily = createTypeColumnFamily(compressionType);
    val sizeColumnFamily = createSizeColumnFamily(compressionType);
    val table = new HTableDescriptor(TableName.valueOf(tableName));
    table.addFamily(typeColumnFamily);
    table.addFamily(sizeColumnFamily);

    return table;
  }

  private HColumnDescriptor createSizeColumnFamily(Algorithm compressionType) {

    return createColumnFamily(compressionType, META_SIZE_INFO_FAMILY);
  }

  private HColumnDescriptor createTypeColumnFamily(Algorithm compressionType) {

    return createColumnFamily(compressionType, META_TYPE_INFO_FAMILY);
  }

  private HColumnDescriptor createColumnFamily(Algorithm compressionType, byte[] familyName) {
    val column = new HColumnDescriptor(familyName);
    column.setBlockCacheEnabled(true);
    column.setInMemory(true);
    column.setBlocksize(META_BLOCK_SIZE);
    column.setBloomFilterType(BloomType.ROWCOL);
    column.setCompressionType(compressionType);
    column.setMaxVersions(1);

    return column;
  }

  public static byte[] encodedRowKey(int donorId, long index) {

    return Bytes.add(Bytes.toBytes(donorId), Bytes.toBytes(index));
  }

  public static CompositeRowKey decodeRowKey(byte[] encodedRowKey) {
    int donorId = Bytes.toInt(encodedRowKey, 0);
    long index = Bytes.toLong(encodedRowKey, 4);

    return new CompositeRowKey(donorId, index);
  }

  public static List<byte[]> calculateBoundaries(List<byte[]> keys) {
    long total = 0;
    val keysPerRS = total / NUM_REGIONS + 1;
    val rangeMap = TreeRangeMap.<Long, CompositeRowKey> create();
    val builder = ImmutableList.<byte[]> builder();

    keys.sort(UnsignedBytes.lexicographicalComparator());

    for (val rowKey : keys) {
      val key = HTableManager.decodeRowKey(rowKey);
      val openRange = total;
      total = total + key.getIndex();
      rangeMap.put(Range.openClosed(openRange, total), key);
    }

    for (int i = 1; i < NUM_REGIONS; ++i) {
      val splitPoint = i * keysPerRS;
      val rangeEntry = rangeMap.getEntry(splitPoint);
      if (rangeEntry == null) break;
      val range = rangeEntry.getKey();
      val donorId = rangeEntry.getValue().getDonorId();
      val index = splitPoint - range.lowerEndpoint();
      val split = HTableManager.encodedRowKey(donorId, index);
      builder.add(split);
    } 

    return builder.build();
  }
}
