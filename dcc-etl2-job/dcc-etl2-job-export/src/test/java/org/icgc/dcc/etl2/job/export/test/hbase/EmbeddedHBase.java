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
package org.icgc.dcc.etl2.job.export.test.hbase;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

@Slf4j
@RequiredArgsConstructor
public class EmbeddedHBase {

  /**
   * State.
   */
  @Getter
  private Configuration config;
  private HBaseTestingUtility utility;

  public void startUp() throws Exception {
    this.config = createConfiguration();
    utility = new HBaseTestingUtility(config);

    log.info("Starting mini-cluster...");
    utility.startMiniCluster();
    log.info("Finished starting mini-cluster");
  }

  public void shutDown() throws Exception {
    log.info("Shutting down mini-cluster...");
    utility.shutdownMiniCluster();
    utility = null;
    config = null;
    log.info("Finished shutting down mini-cluster");
  }

  @SneakyThrows
  public HBaseAdmin createAdmin() {
    return new HBaseAdmin(config);
  }

  @SneakyThrows
  public HTable getTable(String tableName) {
    return new HTable(config, tableName);
  }

  @SneakyThrows
  public int getRowCount(String tableName) {
    return utility.countRows(getTable(tableName));
  }

  private Configuration createConfiguration() {
    log.info("Creating configuation...'");
    val config = HBaseConfiguration.create();

    return config;
  }

}