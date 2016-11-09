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

import static com.google.common.base.Preconditions.checkState;
import static lombok.AccessLevel.PRIVATE;

import java.io.File;
import java.io.FileReader;

import javax.sql.DataSource;

import lombok.Cleanup;
import lombok.NoArgsConstructor;
import lombok.val;

import org.apache.ibatis.jdbc.ScriptRunner;

import com.opentable.db.postgres.embedded.EmbeddedPostgres;

@NoArgsConstructor(access = PRIVATE)
public final class PostgresTests {

  public static final String SQL_FILE = "src/test/resources/sql/fathmm.sql";
  public static final String USER_PASSWD = "postgres";

  public static String getJdbcUrl(EmbeddedPostgres postgres) {
    return postgres.getJdbcUrl(USER_PASSWD, USER_PASSWD);
  }

  public static void initDb(DataSource dataSource) throws Exception {
    initDb(dataSource, SQL_FILE);
  }

  public static void initDb(DataSource dataSource, String sqlFile) throws Exception {
    val scriptRunner = new ScriptRunner(dataSource.getConnection());
    val dbFile = new File(sqlFile);
    checkState(dbFile.exists(), "%s doesn't exist", dbFile.getAbsolutePath());
    @Cleanup
    val reader = new FileReader(dbFile);
    scriptRunner.runScript(reader);
    scriptRunner.closeConnection();
  }

}
