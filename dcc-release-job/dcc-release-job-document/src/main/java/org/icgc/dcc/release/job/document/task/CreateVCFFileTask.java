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
package org.icgc.dcc.release.job.document.task;

import static org.icgc.dcc.common.core.model.FeatureTypes.FeatureType.SSM_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.PROJECT_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.getTestedTypeCountFieldName;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.icgc.dcc.release.core.config.SnpEffProperties;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.job.JobContext;
import org.icgc.dcc.release.core.resolver.ReferenceGenomeResolver;
import org.icgc.dcc.release.core.task.GenericTask;
import org.icgc.dcc.release.core.task.TaskContext;
import org.icgc.dcc.release.core.task.TaskType;
import org.icgc.dcc.release.job.document.io.FilteredOutputStream;
import org.icgc.dcc.release.job.document.io.HDFSMutationsReader;
import org.icgc.dcc.release.job.document.io.MutationVCFDocumentWriter;
import org.icgc.dcc.release.job.document.util.VCFFileSorter;
import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.databind.node.ObjectNode;

@RequiredArgsConstructor(onConstructor = @__({ @Autowired }))
public class CreateVCFFileTask extends GenericTask {

  @Override
  public TaskType getType() {
    return TaskType.FILE_TYPE;
  }

  /**
   * See
   * https://wiki.oicr.on.ca/display/DCCSOFT/Aggregated+Data+Download+Specification?focusedCommentId=57774680#comment
   * -57774680
   */
  public static final String VCF_FILE_NAME = "simple_somatic_mutation.aggregated.vcf.gz";
  private final File tmpVcfFile = createTempFile();
  private final File tmpVcfHeaderFile = createTempFile();

  @NonNull
  private final SnpEffProperties properties;

  @Override
  @SneakyThrows
  public void execute(TaskContext taskContext) {
    createVcfFiles(taskContext);
    saveVcfFiles(taskContext);
  }

  private void saveVcfFiles(TaskContext taskContext) throws IOException {
    val fileSorter = new VCFFileSorter(tmpVcfFile, tmpVcfHeaderFile);
    @Cleanup
    val hdfsOutputStream = createOutputStream(taskContext);
    fileSorter.sortAndSave(hdfsOutputStream);
  }

  private void createVcfFiles(TaskContext taskContext) throws IOException {
    resolveTotalSsmTestedDonorCount(taskContext);
    val mutationsReader = createMutationsReader(taskContext);
    @Cleanup
    val outputStream = createOutputStream();
    @Cleanup
    val writer = createMutationWriter(taskContext, resolveTotalSsmTestedDonorCount(taskContext), outputStream);

    val mutationsIterator = mutationsReader.createMutationsIterator();
    while (mutationsIterator.hasNext()) {
      writer.write(mutationsIterator.next());
    }
  }

  @SneakyThrows
  private static File createTempFile() {
    val tmpFile = File.createTempFile("vcf-file", "-tmp");
    tmpFile.deleteOnExit();

    return tmpFile;
  }

  private Integer resolveTotalSsmTestedDonorCount(TaskContext taskContext) {
    val projects = readProjects(taskContext);

    return projects
        .map(r -> r.get(PROJECT_SUMMARY).get(getTestedTypeCountFieldName(SSM_TYPE)).asInt())
        .reduce((a, b) -> a + b);
  }

  private JavaRDD<ObjectNode> readProjects(TaskContext taskContext) {
    return readInput(taskContext, FileType.PROJECT_SUMMARY);
  }

  private static HDFSMutationsReader createMutationsReader(TaskContext taskContext) {
    return new HDFSMutationsReader(taskContext.getJobContext().getWorkingDir(), taskContext.getFileSystem());
  }

  @SneakyThrows
  private MutationVCFDocumentWriter createMutationWriter(TaskContext taskContext, Integer totalSsmTestedDonorCount,
      OutputStream outputStream) {
    val jobContext = taskContext.getJobContext();

    return new MutationVCFDocumentWriter(jobContext.getReleaseName(), resolveFastaFile(), outputStream,
        totalSsmTestedDonorCount);
  }

  private File resolveFastaFile() {
    val resolver = new ReferenceGenomeResolver(
        properties.getResourceDir(),
        properties.getResourceUrl(),
        properties.getReferenceGenomeVersion());

    return resolver.resolve();
  }

  @SneakyThrows
  private static OutputStream createOutputStream(TaskContext taskContext) {
    val vcfPath = resolveVcfPath(taskContext.getJobContext());

    return new GZIPOutputStream(new BufferedOutputStream(taskContext.getFileSystem().create(vcfPath)));
  }

  @SneakyThrows
  private OutputStream createOutputStream() {
    return new FilteredOutputStream(tmpVcfHeaderFile, tmpVcfFile);
  }

  private static Path resolveVcfPath(JobContext jobContext) {
    return new Path(jobContext.getWorkingDir(), VCF_FILE_NAME);
  }

}
