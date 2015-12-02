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
package org.icgc.dcc.release.job.export.model;

import static lombok.AccessLevel.PRIVATE;
import lombok.NoArgsConstructor;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;

/**
 * Shamefully forked from:
 * 
 * <pre>
 * https://github.com/icgc-dcc/dcc-downloader/blob/develop/dcc-downloader-core/src/main/java/org/icgc/dcc/downloader/core/SchemaUtil.java
 * </pre>
 */
@NoArgsConstructor(access = PRIVATE)
public final class ExportTables {

  public static final int NUM_REGIONS = 60;

  /**
   * Data table.
   */
  public static final byte[] DATA_CONTENT_FAMILY = new byte[] { 'd' };
  public static final int DATA_BLOCK_SIZE = 5242880;
  public static final int MAX_DATA_FILE_SIZE = 524288000;
  public static final String DATA_TYPE_SEPARATOR = ",";

  /**
   * Meta table.
   */
  public static final String META_TABLE_NAME = "meta";
  public static final byte[] META_TYPE_INFO_FAMILY = new byte[] { 't' };
  public static final byte[] META_TYPE_HEADER = new byte[] { 'h' };
  public static final String HEADER_SEPARATOR = ",";
  public static final int META_BLOCK_SIZE = 65536;
  public static final byte[] META_SIZE_INFO_FAMILY = new byte[] { 's' };

  public static final String TABLENAME_SEPARATOR = ".";
  public static final byte[] TSV_DELIMITER = new byte[] { '\t' };
  public static final long MAX_TAR_ENTRY_SIZE_IN_BYTES = 3221225472L;
  public static final byte[] END_OF_LINE = new byte[] { 10 }; // LF character

  /**
   * Paths
   */
  public static final String ROOT = "/tmp/download/";
  public static final String ROOT_OUT_STATIC = ROOT + "static/";
  public static final String TMP = ROOT + "tmp/";
  public static final String TMP_DYNAMIC_ROOT = TMP + "dynamic/";
  public static final String TMP_HFILE_ROOT = TMP + "hfile/";
  public static final String TMP_STATIC_ROOT = TMP + "static/";
  public static final String TMP_BUCKET_ROOT = TMP + "bucket/";
  public static final String TMP_INDEX = TMP + "fullindex/";
  public static final String OUT_DYNAMIC = ROOT + "dynamic/";

  /**
   * Misc.
   */
  public static final String ICGC_DONOR_ID_PREFIX = "DO";
  public static final FsPermission rwx = new FsPermission("777");
  public static final int BLOCKSIZE = 5 * 1048576;
  public static final Algorithm COMPRESSION = Compression.Algorithm.SNAPPY;

  public static String getStaticFileOutput(String tableName) {
    return TMP_STATIC_ROOT + tableName;
  }

  public static String getTableName(String tableName, String release) {
    return tableName + "-" + release;
  }

}
