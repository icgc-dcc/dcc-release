package org.icgc.dcc.release.job.id.dump.impl;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.sqoop.Sqoop;
import org.icgc.dcc.release.job.id.config.PostgresqlProperties;
import org.icgc.dcc.release.job.id.dump.DumpDataToHDFS;

import java.io.IOException;

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

public class DumpBySqoop implements DumpDataToHDFS {

  private final FileSystem fileSystem;
  private final PostgresqlProperties postgresqlProperties;
  private final String workingDir;

  public DumpBySqoop(FileSystem system, PostgresqlProperties postgresql, String dir) {
    this.fileSystem = system;
    this.postgresqlProperties = postgresql;
    this.workingDir = dir;
  }

  @Override
  public boolean dump() {


    try {
      String target = this.workingDir + "/pg_dump/mutation";

      this.fileSystem.delete(new Path(target), true);

      String[] paras = new String[] {
          "import",
          "--connect",
          "jdbc:postgresql://" + this.postgresqlProperties.getServer() + "/dcc_identifier",
          "--username",
          "dcc",
          "--password",
          this.postgresqlProperties.getPassword(),
          "--table",
          "mutation_ids",
          "--target-dir",
          target,
          "--delete-target-dir"
      };

      int code = Sqoop.runTool(paras, this.fileSystem.getConf());

      return code==0?true:false;

    } catch (IOException e) {
      e.printStackTrace();
    }

    return false;

  }
}
