package org.icgc.dcc.release.job.id.dump.impl;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.release.job.id.config.PostgresqlProperties;
import org.icgc.dcc.release.job.id.dump.DumpDataToHDFS;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Copyright (c) $today.year The Ontario Institute for Cancer Research. All rights reserved.
 * <p>
 * This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
 * You should have received a copy of the GNU General Public License along with
 * this program. If not, see <http://www.gnu.org/licenses/>.
 * <p>
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

public class DumpMutationDataByPGCopyManager implements DumpDataToHDFS {

  private final FileSystem fileSystem;
  private final PostgresqlProperties postgresqlProperties;
  private final String dumpTarget;

  public DumpMutationDataByPGCopyManager(FileSystem system, PostgresqlProperties postgresql, String target) {
    this.fileSystem = system;
    this.postgresqlProperties = postgresql;
    this.dumpTarget = target;
  }
  @Override
  public boolean dump() {
    try {

      Path target = new Path(this.dumpTarget);

      this.fileSystem.delete(target, true);

      FSDataOutputStream outputStream = this.fileSystem.create(target);


      Connection conn = DriverManager.getConnection("jdbc:postgresql://" + postgresqlProperties.getServer() + "/" + postgresqlProperties.getDatabase(), postgresqlProperties.getUser(), postgresqlProperties.getPassword());
      CopyManager copyManager = ((PGConnection)conn).getCopyAPI();
      copyManager.copyOut("COPY mutation_ids TO STDOUT ", outputStream);

      outputStream.flush();
      outputStream.close();

      return true;

    } catch (SQLException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return false;
  }
}
