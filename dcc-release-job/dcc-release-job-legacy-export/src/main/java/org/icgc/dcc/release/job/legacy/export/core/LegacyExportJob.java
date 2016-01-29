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
package org.icgc.dcc.release.job.legacy.export.core;

import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.checkExistence;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.mkdirs;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.rmr;

import java.util.Collection;
import java.util.List;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.job.GenericJob;
import org.icgc.dcc.release.core.job.JobContext;
import org.icgc.dcc.release.core.job.JobType;
import org.icgc.dcc.release.core.task.Task;
import org.icgc.dcc.release.job.legacy.export.task.LegacyExportTask;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableList;

@Slf4j
@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class LegacyExportJob extends GenericJob {

  public static final String EXPORT_DIR = "export";
  public static final List<FileType> EXPORT_FILE_TYPES = ImmutableList.of(
      FileType.CLINICAL,
      FileType.SSM,
      FileType.CNSM,
      FileType.STSM,
      FileType.SGV,
      FileType.PEXP,
      FileType.METH_ARRAY,
      FileType.METH_SEQ,
      FileType.MIRNA_SEQ,
      FileType.JCN,
      FileType.EXP_ARRAY,
      FileType.EXP_SEQ);

  @NonNull
  private final FileSystem fileSystem;

  @Override
  public JobType getType() {
    return JobType.LEGACY_EXPORT;
  }

  @Override
  public void execute(JobContext jobContext) {
    clean(jobContext);
    legacyExport(jobContext);
  }

  private void clean(JobContext jobContext) {
    val outputDir = getOutputDir(jobContext);

    if (checkExistence(fileSystem, outputDir)) {
      log.info("Deleting directory {} ...", outputDir);
      rmr(fileSystem, outputDir);
    }

    log.info("Creating directory {} ...", outputDir);
    mkdirs(fileSystem, outputDir);
  }

  private static void legacyExport(JobContext jobContext) {
    val tasks = createTasks();
    jobContext.execute(tasks);
  }

  private static Collection<Task> createTasks() {
    return EXPORT_FILE_TYPES.stream()
        .map(fileType -> new LegacyExportTask(fileType))
        .collect(toImmutableList());
  }

  private static Path getOutputDir(JobContext jobContext) {
    return new Path(new Path(jobContext.getWorkingDir()), EXPORT_DIR);
  }

  public static FileType getOutputFileType(FileType inputFileType) {
    return FileType.CLINICAL == inputFileType ? FileType.DONOR : inputFileType;
  }

}
