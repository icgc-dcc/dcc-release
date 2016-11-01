/*
 * Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.release.job.fathmm.util;

import javax.sql.DataSource;

import lombok.SneakyThrows;
import lombok.val;

import org.junit.Before;
import org.junit.Rule;
import org.postgresql.ds.PGSimpleDataSource;

import ru.yandex.qatools.embed.postgresql.config.PostgresConfig;

public abstract class AbstractPostgresqlTest {

  protected DataSource dataSource;
  protected PostgresConfig config = initPostgresConfig();

  @Rule
  public final EmbeddedPostgres embeddedPostgres = new EmbeddedPostgres(config);

  @SneakyThrows
  private PostgresConfig initPostgresConfig() {
    return PostgresConfig.defaultWithDbName("test", "test", "test");
  }

  @Before
  public void setUp() throws Exception {
    val dataSource = new PGSimpleDataSource();
    dataSource.setServerName(config.net().host());
    dataSource.setPortNumber(config.net().port());
    dataSource.setDatabaseName(config.storage().dbName());
    dataSource.setUser(config.credentials().username());
    dataSource.setPassword(config.credentials().password());

    this.dataSource = dataSource;
  }

}
