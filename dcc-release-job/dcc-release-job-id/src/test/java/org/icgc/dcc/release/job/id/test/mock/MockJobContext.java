package org.icgc.dcc.release.job.id.test.mock;

import com.google.common.collect.Table;
import lombok.Value;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.icgc.dcc.release.core.job.JobContext;
import org.icgc.dcc.release.core.job.JobType;
import org.icgc.dcc.release.core.task.Task;

import java.util.Collection;
import java.util.List;

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

public class MockJobContext implements JobContext {

  private JavaSparkContext jsc;
  private FileSystem fs;

  public MockJobContext(JavaSparkContext sc, FileSystem system){
    this.jsc = sc;
    this.fs = system;
  }

  @Override
  public JobType getType() {
    return null;
  }

  @Override
  public List<String> getProjectNames() {
    return null;
  }

  @Override
  public String getReleaseDir() {
    return null;
  }

  @Override
  public String getReleaseName() {
    return null;
  }

  @Override
  public String getWorkingDir() {
    return "/icgc/release/ICGC24/0";
  }

  @Override
  public Table<String, String, List<Path>> getFiles() {
    return null;
  }

  @Override
  public void execute(Task... tasks) {

  }

  @Override
  public void execute(Collection<? extends Task> tasks) {

  }

  @Override
  public void executeSequentially(Task... tasks) {

  }

  @Override
  public void executeSequentially(Collection<? extends Task> tasks) {

  }

  @Override
  public boolean isCompressOutput() {
    return false;
  }

  @Override
  public JavaSparkContext getJavaSparkContext() {
    return null;
  }

  @Override
  public FileSystem getFileSystem() {
    return null;
  }
}
