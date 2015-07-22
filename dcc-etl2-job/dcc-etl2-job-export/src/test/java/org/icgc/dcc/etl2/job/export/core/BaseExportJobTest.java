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
package org.icgc.dcc.etl2.job.export.core;

import java.io.File;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl2.job.export.test.hbase.EmbeddedHBase;
import org.icgc.dcc.etl2.test.job.AbstractJobTest;
import org.junit.After;
import org.junit.Before;

@Slf4j
public abstract class BaseExportJobTest extends AbstractJobTest {

  /**
   * Configuration.
   */
  private File loaderDir;

  /**
   * Collaborators.
   */
  private EmbeddedHBase hbase;

  @Override
  @Before
  @SneakyThrows
  public void setUp() {
    super.setUp();
    this.hbase = new EmbeddedHBase();

    log.info("> Starting embedded HBase...");
    hbase.startUp();
    log.info("< Started embedded HBase");

    log.info("> Creating loader directory...");
    this.loaderDir = tmp.newFolder();
    log.info("< Created loader directory: {}", loaderDir);
  }

  @Override
  @SneakyThrows
  @After
  public void shutDown() {
    super.shutDown();
    log.info("> Stopping embedded HBase...");
    hbase.shutDown();
    hbase = null;
    log.info("< Stopped embedded HBase");
  }

}