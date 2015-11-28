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
package org.icgc.dcc.release.job.document.core;

import static java.lang.String.format;
import static org.icgc.dcc.release.job.document.util.DocumentTypes.getBroadcastDependencies;
import static org.icgc.dcc.release.job.document.util.DocumentTypes.getIndexClassName;

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Map;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

import org.icgc.dcc.release.core.config.SnpEffProperties;
import org.icgc.dcc.release.core.document.DocumentType;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.job.GenericJob;
import org.icgc.dcc.release.core.job.JobContext;
import org.icgc.dcc.release.core.job.JobType;
import org.icgc.dcc.release.core.task.Task;
import org.icgc.dcc.release.job.document.config.DocumentProperties;
import org.icgc.dcc.release.job.document.core.DocumentJobContext.DocumentJobContextBuilder;
import org.icgc.dcc.release.job.document.model.BroadcastType;
import org.icgc.dcc.release.job.document.task.CreateVCFFileTask;
import org.icgc.dcc.release.job.document.task.ResolveDonorsTask;
import org.icgc.dcc.release.job.document.task.ResolveGenesTask;
import org.icgc.dcc.release.job.document.task.ResolveProjectsTask;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

@Component
@RequiredArgsConstructor(onConstructor = @__({ @Autowired }))
public class DocumentJob extends GenericJob {

  /**
   * Dependencies.
   */
  @NonNull
  private final DocumentProperties properties;
  @NonNull
  private final SnpEffProperties snpEffProperties;

  @Override
  public JobType getType() {
    return JobType.DOCUMENT;
  }

  @Override
  public void execute(@NonNull JobContext jobContext) {
    clean(jobContext);
    write(jobContext);
  }

  private void clean(JobContext jobContext) {
    delete(jobContext, resolveOutputFileTypes());
  }

  private static FileType[] resolveOutputFileTypes() {
    val outputFileTypes = Lists.<FileType> newArrayList();
    for (val documentType : DocumentType.values()) {
      outputFileTypes.add(documentType.getOutputFileType());
    }

    return outputFileTypes.toArray(new FileType[outputFileTypes.size()]);
  }

  static String resolveIndexName(String releaseName) {
    return "test-release-" + releaseName.toLowerCase();
  }

  private void write(JobContext jobContext) {
    for (val task : createStreamingTasks(jobContext)) {
      jobContext.execute(task);
    }

    // Generate SSM VCF file
    if (properties.isExportVCF()) {
      jobContext.execute(new CreateVCFFileTask(snpEffProperties));
    }
  }

  @SneakyThrows
  private Collection<? extends Task> createStreamingTasks(JobContext jobContext) {
    val tasks = ImmutableList.<Task> builder();
    for (val documentType : DocumentType.values()) {
      val constructor = getConstructor(documentType);
      val indexJobContext = createIndexJobContext(jobContext, documentType);
      tasks.add((Task) constructor.newInstance(indexJobContext));
    }

    return tasks.build();
  }

  private static Constructor<?> getConstructor(DocumentType documentType) throws ClassNotFoundException,
      NoSuchMethodException {
    val indexClassName = getIndexClassName(documentType);
    val clazz = Class.forName(indexClassName);
    val constructor = clazz.getConstructor(DocumentJobContext.class);

    return constructor;
  }

  private DocumentJobContext createIndexJobContext(JobContext jobContext, DocumentType documentType) {
    val indexJobBuilder = DocumentJobContext.builder();
    val indexJobDependencies = resolveDependencies(jobContext, documentType);
    setDependencies(indexJobBuilder, indexJobDependencies);

    return indexJobBuilder.build();
  }

  private static void setDependencies(DocumentJobContextBuilder indexJobBuilder,
      Map<BroadcastType, ? extends Task> indexJobDependencies) {
    for (val entry : indexJobDependencies.entrySet()) {
      switch (entry.getKey()) {
      case PROJECT:
        val resolveProjectsTask = (ResolveProjectsTask) entry.getValue();
        indexJobBuilder.projectsBroadcast(resolveProjectsTask.getProjectsBroadcast());
        break;
      case DONOR:
        val resolveDonorsTask = (ResolveDonorsTask) entry.getValue();
        indexJobBuilder.donorsBroadcast(resolveDonorsTask.getDonorsBroadcast());
        break;
      case GENE:
        val resolveGenesTask = (ResolveGenesTask) entry.getValue();
        indexJobBuilder.genesBroadcast(resolveGenesTask.getGenesBroadcast());
        break;
      default:
        throw new IllegalArgumentException(format("Unrecoginzed broadcast type %s", entry.getKey()));
      }
    }
  }

  private static Map<BroadcastType, ? extends Task> resolveDependencies(JobContext jobContext,
      DocumentType documentType) {
    val tasksBuilder = ImmutableMap.<BroadcastType, Task> builder();
    for (val dependencyTask : getBroadcastDependencies(documentType)) {
      tasksBuilder.put(dependencyTask, createDependentyTask(dependencyTask, documentType));
    }

    val tasks = tasksBuilder.build();
    jobContext.execute(tasks.values());

    return tasks;
  }

  @SneakyThrows
  private static Task createDependentyTask(BroadcastType dependencyTask, DocumentType documentType) {
    val clazz = dependencyTask.getDependencyClass();
    val constructor = clazz.getConstructor(DocumentType.class);

    return constructor.newInstance(documentType);
  }

}