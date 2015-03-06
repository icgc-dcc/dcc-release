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
package org.icgc.dcc.etl2.job.export.model;

import static lombok.AccessLevel.PRIVATE;
import lombok.NoArgsConstructor;

/**
 * Shamefully forked from:
 * 
 * <pre>
 * https://github.com/icgc-dcc/dcc-downloader/blob/develop/dcc-downloader-core/src/main/java/org/icgc/dcc/downloader/core/SchemaUtil.java
 * </pre>
 */
@NoArgsConstructor(access = PRIVATE)
public final class ExportTables {

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

}