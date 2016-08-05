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

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static org.icgc.dcc.common.core.model.FeatureTypes.FeatureType.SSM_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.PROJECT_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.getTestedTypeCountFieldName;
import static org.icgc.dcc.release.core.util.Partitions.getPartitionsCount;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.Collections;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

import org.apache.spark.api.java.JavaRDD;
import org.icgc.dcc.release.core.config.SnpEffProperties;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.resolver.ReferenceGenomeResolver;
import org.icgc.dcc.release.core.task.GenericTask;
import org.icgc.dcc.release.core.task.TaskContext;
import org.icgc.dcc.release.core.task.TaskType;
import org.icgc.dcc.release.core.util.Configurations;
import org.icgc.dcc.release.job.document.function.MutationVCFConverter;
import org.icgc.dcc.release.job.document.function.SaveVCFRecords;
import org.icgc.dcc.release.job.document.io.MutationVCFDocumentWriter;
import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.databind.node.ObjectNode;

@RequiredArgsConstructor(onConstructor = @__({ @Autowired }))
public class CreateVCFFileTask extends GenericTask {

  @NonNull
  private final SnpEffProperties properties;

  @Override
  public TaskType getType() {
    return TaskType.FILE_TYPE;
  }

  @Override
  @SneakyThrows
  public void execute(TaskContext taskContext) {
    val testedDonorCount = resolveTotalSsmTestedDonorCount(taskContext);
    val releaseName = taskContext.getJobContext().getReleaseName();
    val fastaFile = resolveFastaFile();

    val header = getVCFHeaderRDD(testedDonorCount, releaseName, fastaFile, taskContext);

    val input = readDocInput(taskContext, FileType.MUTATION_CENTRIC_DOCUMENT);
    val partitionsCount = getPartitionsCount(input);
    val records = input.mapPartitions(new MutationVCFConverter(testedDonorCount, releaseName, properties))
        .sortBy(key -> key, true, partitionsCount);

    val output = header.union(records);
    save(output, taskContext);
  }

  private void save(JavaRDD<String> output, TaskContext taskContext) {
    val workingDir = taskContext.getJobContext().getWorkingDir();
    val fileSystemSettings = Configurations.getSettings(taskContext.getFileSystem().getConf());

    output.coalesce(1)
        .mapPartitions(new SaveVCFRecords(workingDir, fileSystemSettings))
        .count();
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

  private File resolveFastaFile() {
    val resolver = new ReferenceGenomeResolver(
        properties.getResourceDir(),
        properties.getResourceUrl(),
        properties.getReferenceGenomeVersion());

    return resolver.resolve();
  }

  @SneakyThrows
  private static JavaRDD<String> getVCFHeaderRDD(int testedDonorCount, String releaseName, File fastaFile,
      TaskContext taskContext) {
    val buffer = new ByteArrayOutputStream();
    new MutationVCFDocumentWriter(releaseName, fastaFile, buffer, testedDonorCount);
    val header = buffer.toString();
    checkState(!isNullOrEmpty(header), "Expected non-empty VCF header");
    val sparkContext = taskContext.getSparkContext();

    return sparkContext.parallelize(Collections.singletonList(header));
  }

}
