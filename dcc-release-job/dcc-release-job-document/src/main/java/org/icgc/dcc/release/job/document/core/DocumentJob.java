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
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;
import static org.icgc.dcc.release.job.document.util.DocumentTypes.getBroadcastDependencies;
import static org.icgc.dcc.release.job.document.util.DocumentTypes.getDocumentClassName;

import java.lang.reflect.Constructor;
import java.util.Map;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
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
import org.icgc.dcc.release.job.document.task.ResolveClinvarTask;
import org.icgc.dcc.release.job.document.task.ResolveCivicTask;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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
  @NonNull
  private final JavaSparkContext sparkContext;

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

  private void write(JobContext jobContext) {
    for (val documentType : getDocumentTypes()) {
      val documentJobContext = createDocumentJobContext(jobContext, documentType);
      jobContext.execute(createStreamingTask(jobContext, documentType, documentJobContext));
      destroyBroadcasts(documentJobContext);
    }

    // Generate SSM VCF file
    if (properties.isExportVCF()) {
      jobContext.execute(new CreateVCFFileTask(snpEffProperties));
    }
  }

  private Iterable<DocumentType> getDocumentTypes() {
    val includeTypes = properties.getIncludeTypes();

    return includeTypes.isEmpty() ?
        ImmutableSet.copyOf(DocumentType.values()) :
        includeTypes.stream()
            .map(type -> DocumentType.valueOf(type.toUpperCase()))
            .collect(toImmutableSet());
  }

  @SneakyThrows
  private Task createStreamingTask(JobContext jobContext, DocumentType documentType, DocumentJobContext documentJobContext) {
    val constructor = getConstructor(documentType);

    return (Task) constructor.newInstance(documentJobContext);
  }

  private static Constructor<?> getConstructor(DocumentType documentType) throws ClassNotFoundException,
      NoSuchMethodException {
    val documentClassName = getDocumentClassName(documentType);
    val clazz = Class.forName(documentClassName);
    val constructor = clazz.getConstructor(DocumentJobContext.class);

    return constructor;
  }

  private DocumentJobContext createDocumentJobContext(JobContext jobContext, DocumentType documentType) {
    val documentJobBuilder = DocumentJobContext.builder();
    val documentJobDependencies = resolveDependencies(jobContext, documentType);
    setDependencies(documentJobBuilder, documentJobDependencies);

    return documentJobBuilder.build();
  }

  private void setDependencies(DocumentJobContextBuilder documentJobBuilder,
      Map<BroadcastType, ? extends Task> documentJobDependencies) {
    for (val entry : documentJobDependencies.entrySet()) {
      switch (entry.getKey()) {
      case PROJECT:
        val resolveProjectsTask = (ResolveProjectsTask) entry.getValue();
        documentJobBuilder.projectsBroadcast(createBroadcast(resolveProjectsTask.getProjectIdProjects()));
        break;
      case DONOR:
        val resolveDonorsTask = (ResolveDonorsTask) entry.getValue();
        documentJobBuilder.donorsBroadcast(createBroadcast(resolveDonorsTask.getProjectDonors()));
        break;
      case GENE:
        val resolveGenesTask = (ResolveGenesTask) entry.getValue();
        documentJobBuilder.genesBroadcast(createBroadcast(resolveGenesTask.getGeneIdGenes()));
        break;
      case CLINVAR:
        val resolveClinvarTask = (ResolveClinvarTask) entry.getValue();
        documentJobBuilder.clinvarBroadcast(createBroadcast(resolveClinvarTask.getAnnotationIdClinvar()));
        break;
      case CIVIC:
        val resolveCivicTask = (ResolveCivicTask) entry.getValue();
        documentJobBuilder.civicBroadcast(createBroadcast(resolveCivicTask.getAnnotationIdCivic()));
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

  private <T> Broadcast<T> createBroadcast(T value) {
    return sparkContext.broadcast(value);
  }

  private static void destroyBroadcasts(DocumentJobContext documentJobContext) {
    val projects = documentJobContext.getProjectsBroadcast();
    if (projects != null) {
      projects.destroy(false);
    }

    val donors = documentJobContext.getDonorsBroadcast();
    if (donors != null) {
      donors.destroy(false);
    }

    val genes = documentJobContext.getGenesBroadcast();
    if (genes != null) {
      genes.destroy(false);
    }

    val clinvar = documentJobContext.getClinvarBroadcast();
    if (clinvar != null) {
      clinvar.destroy(false);
    }

    val civic = documentJobContext.getCivicBroadcast();
    if (civic != null) {
      civic.destroy(false);
    }
  }

}