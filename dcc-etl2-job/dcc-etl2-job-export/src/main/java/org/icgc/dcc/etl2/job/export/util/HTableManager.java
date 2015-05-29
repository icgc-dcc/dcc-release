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

import static org.icgc.dcc.etl2.job.export.model.ExportTables.DATA_BLOCK_SIZE;
import static org.icgc.dcc.etl2.job.export.model.ExportTables.DATA_CONTENT_FAMILY;
import static org.icgc.dcc.etl2.job.export.model.ExportTables.MAX_DATA_FILE_SIZE;
import static org.icgc.dcc.etl2.job.export.model.ExportTables.NUM_REGIONS;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.icgc.dcc.etl2.job.export.model.CompositeRowKey;

import scala.Tuple3;

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
public class HTableManager {

  /**
   * Constants
   */
  private final boolean USE_SNAPPY = false;

  /**
   * Dependencies.
   */
  private final Configuration conf;
  private final HBaseAdmin admin;

  @SneakyThrows
  public HTableManager(Configuration conf) {
    this.conf = conf;
    this.admin = new HBaseAdmin(conf);
  }

  @SneakyThrows
  public HTable ensureTable(@NonNull String tableName, @NonNull List<Tuple3<String, Long, Integer>> stats) {
    if (!admin.tableExists(tableName)) {
      log.info("Calculating split keys...");
      val splitKeys =
          stats.stream().map(stat -> encodedRowKey(Integer.valueOf(stat._1()), stat._3())).collect(Collectors.toList());
      log.info("Finished calculating split keys...");

      log.info("Creating table...");
      createDataTable(tableName, calculateBoundaries(splitKeys), conf, USE_SNAPPY);
      log.info("Finished creating table...");
    }

    return new HTable(conf, tableName);
  }

  @SneakyThrows
  public HTable getTable(@NonNull String tableName) {
    if (!admin.tableExists(tableName)) {
      throw new IOException("table not found.");
    }

    return new HTable(conf, tableName);
  }

  @SneakyThrows
  public boolean existsTable(@NonNull String tableName) {

    return admin.tableExists(tableName);
  }

  @SneakyThrows
  public void listTables() {
    for (val table : admin.listTables()) {
      log.info("{}", table);
    }
  }

  public static byte[] encodedRowKey(int donorId, long sum) {

    return Bytes.add(Bytes.toBytes(donorId), Bytes.toBytes(sum));
  }

  public static CompositeRowKey decodeRowKey(byte[] encodedRowKey) {
    int donorId = Bytes.toInt(encodedRowKey, 0);
    long sum = Bytes.toLong(encodedRowKey, 4);

    return new CompositeRowKey(donorId, sum);
  }

  public static List<byte[]> calculateBoundaries(List<byte[]> keys) {
    long current = 0;
    val rangeMap = TreeRangeMap.<Long, CompositeRowKey> create();
    val builder = ImmutableList.<byte[]> builder();

    keys.sort(UnsignedBytes.lexicographicalComparator());

    for (val rowKey : keys) {
      val key = decodeRowKey(rowKey);
      val splitStart = current;
      current = current + key.getIndex();
      rangeMap.put(Range.openClosed(splitStart, current), key);
    }

    val keysPerRS = current / NUM_REGIONS + 1;
    for (int i = 1; i < NUM_REGIONS; ++i) {
      val splitPoint = i * keysPerRS;
      val rangeEntry = rangeMap.getEntry(splitPoint);
      if (rangeEntry == null) break;
      val range = rangeEntry.getKey();
      val donorId = rangeEntry.getValue().getDonorId();
      val sum = splitPoint - range.lowerEndpoint();
      val split = HTableManager.encodedRowKey(donorId, sum);
      builder.add(split);
    }

    return builder.build();
  }

  public static void createDataTable(String tableName) throws IOException {
    createDataTable(tableName, ImmutableList.<byte[]> of(),
        HBaseConfiguration.create(), true);
  }

  public static void createDataTable(String tableName, Configuration conf)
      throws IOException {
    createDataTable(tableName, ImmutableList.<byte[]> of(), conf, true);
  }

  public static void createDataTable(String tableName, Configuration conf,
      boolean withSnappyCompression) throws IOException {
    createDataTable(tableName, ImmutableList.<byte[]> of(), conf,
        withSnappyCompression);
  }

  @SuppressWarnings("deprecation")
  public static void createDataTable(String tableName,
      List<byte[]> boundaries, Configuration conf,
      boolean withSnappyCompression) throws IOException {
    HBaseAdmin admin = new HBaseAdmin(conf);
    try {
      if (!admin.tableExists(tableName)) {
        byte[][] splits = new byte[boundaries.size()][];
        HTableDescriptor descriptor = new HTableDescriptor(tableName);
        HColumnDescriptor dataSchema = new HColumnDescriptor(
            DATA_CONTENT_FAMILY);
        dataSchema.setBlockCacheEnabled(false);
        dataSchema.setBlocksize(DATA_BLOCK_SIZE);
        dataSchema.setBloomFilterType(BloomType.ROW);
        if (withSnappyCompression) dataSchema.setCompressionType(Algorithm.SNAPPY);
        dataSchema.setMaxVersions(1);
        descriptor.addFamily(dataSchema);
        descriptor.setMaxFileSize(MAX_DATA_FILE_SIZE);
        admin.createTable(descriptor, boundaries.toArray(splits));
      }
    } catch (TableExistsException e) {
      log.warn("already created... (skip)", e);
    } finally {
      admin.close();
    }
  }
}
