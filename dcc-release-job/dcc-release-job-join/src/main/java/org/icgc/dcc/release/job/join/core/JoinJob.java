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
package org.icgc.dcc.release.job.join.core;

import static java.lang.System.getProperty;
import static java.util.Collections.emptyList;
import static org.icgc.dcc.common.core.util.Splitters.COMMA;
import static org.icgc.dcc.release.core.job.FileType.CLINICAL;
import static org.icgc.dcc.release.core.job.FileType.OBSERVATION;
import static org.icgc.dcc.release.core.job.FileType.SSM_P;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.broadcast.Broadcast;
import org.icgc.dcc.common.core.model.FieldNames;
import org.icgc.dcc.common.core.util.Joiners;
import org.icgc.dcc.common.core.util.stream.Collectors;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.job.GenericJob;
import org.icgc.dcc.release.core.job.JobContext;
import org.icgc.dcc.release.core.job.JobType;
import org.icgc.dcc.release.core.submission.SubmissionFileField;
import org.icgc.dcc.release.core.submission.SubmissionFileSchemas;
import org.icgc.dcc.release.core.task.Task;
import org.icgc.dcc.release.job.join.model.DonorSample;
import org.icgc.dcc.release.job.join.task.ClinicalJoinTask;
import org.icgc.dcc.release.job.join.task.MethArrayJoinTask;
import org.icgc.dcc.release.job.join.task.ObservationJoinTask;
import org.icgc.dcc.release.job.join.task.PrimaryMetaJoinTask;
import org.icgc.dcc.release.job.join.task.ResolveDonorSamplesTask;
import org.icgc.dcc.release.job.join.task.ResolveRawSequenceDataTask;
import org.icgc.dcc.release.job.join.task.ResolveSampleSurrogateSampleIds;
import org.icgc.dcc.release.job.join.task.SecondaryJoinTask;
import org.icgc.dcc.release.job.join.task.SgvJoinTask;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

@Slf4j
@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class JoinJob extends GenericJob {

  /**
   * Metadata.
   */
  @NonNull
  private final SubmissionFileSchemas schemas;

  private static final Set<FileType> ANALYSIS_FILE_TYPES = ImmutableSet.of(
      FileType.MIRNA_SEQ,
      FileType.METH_SEQ,
      FileType.EXP_SEQ,
      FileType.EXP_ARRAY,
      FileType.PEXP,
      FileType.JCN,
      FileType.METH_ARRAY,
      FileType.CNSM,
      FileType.STSM,
      FileType.SSM,
      FileType.SGV);

  /**
   * Valid tasks the Join Job can execute
   */
  private static final String VALID_TASKS = Joiners.COMMA.join(ANALYSIS_FILE_TYPES);

  /**
   * A coma-separated list of join tasks(file types) to execute.
   */
  private static final String EXECUTE_TASKS_PROPERTY = "joinjob.tasks";

  /**
   * Helps to define what dependencies the task requires.
   */
  private static enum TaskType {
    PRIMARY,
    SECONDARY;
  }

  private static final Map<FileType, TaskType> FILE_TYPE_TASK_TYPE = ImmutableMap.<FileType, TaskType> builder()
      .put(FileType.MIRNA_SEQ_P, TaskType.PRIMARY)
      .put(FileType.METH_SEQ_P, TaskType.PRIMARY)
      .put(FileType.EXP_SEQ_P, TaskType.PRIMARY)
      .put(FileType.EXP_ARRAY_P, TaskType.PRIMARY)
      .put(FileType.PEXP_P, TaskType.PRIMARY)
      .put(FileType.JCN_P, TaskType.PRIMARY)
      .put(FileType.METH_ARRAY_P, TaskType.PRIMARY)

      .put(FileType.CNSM_P, TaskType.SECONDARY)
      .put(FileType.STSM_P, TaskType.SECONDARY)
      .put(FileType.SSM_P, TaskType.SECONDARY)
      .put(FileType.SGV_P, TaskType.SECONDARY)
      .build();

  @Override
  public JobType getType() {
    return JobType.JOIN;
  }

  @Override
  public void execute(@NonNull JobContext jobContext) {
    clean(jobContext);
    join(jobContext);
  }

  private void clean(JobContext jobContext) {
    delete(jobContext, getDeleteFileTypes());
  }

  private void join(JobContext jobContext) {
    val resolveRawSequenceDataTask = new ResolveRawSequenceDataTask();
    jobContext.execute(resolveRawSequenceDataTask);
    val rawSequenceDataBroadcast = resolveRawSequenceDataTask.getRawSequenceDataBroadcast();
    jobContext.execute(new ClinicalJoinTask(rawSequenceDataBroadcast));

    // Discard the broadcast
    rawSequenceDataBroadcast.destroy();
    val executeFileTypes = resolveExecuteFileTypes();

    if (executeFileTypes.isEmpty()) {
      return;
    }

    val resolveDonorSamplesTask = new ResolveDonorSamplesTask();
    val resolveSampleIds = new ResolveSampleSurrogateSampleIds();
    jobContext.execute(resolveDonorSamplesTask);
    val donorSamples = resolveDonorSamplesTask.getDonorSamplesBroadcast();

    val tasks = createTasks(jobContext, executeFileTypes, resolveSampleIds, donorSamples);
    jobContext.execute(tasks);
  }

  private List<Task> createTasks(JobContext jobContext, List<FileType> executeFileTypes,
      ResolveSampleSurrogateSampleIds resolveSampleIds,
      Broadcast<Map<String, Map<String, DonorSample>>> donorSamples) {
    val tasks = ImmutableList.<Task> builder();

    boolean hasResolvedSamples = false;
    for (val executeFileType : executeFileTypes) {
      if (isPrimaryTask(executeFileType)) {
        tasks.add(createPrimaryTask(executeFileType, donorSamples));

      } else {
        if (!hasResolvedSamples) {
          jobContext.execute(resolveSampleIds);
        }

        val sampleSurrogateSampleIds = resolveSampleIds.getSampleSurrogateSampleIdsBroadcast();
        tasks.add(createSecondaryTask(executeFileType, donorSamples, sampleSurrogateSampleIds));
      }
    }

    return tasks.build();
  }

  private Task createSecondaryTask(FileType executeFileType,
      Broadcast<Map<String, Map<String, DonorSample>>> donorSamples,
      Broadcast<Map<String, Map<String, String>>> sampleSurrogateSampleIds) {
    switch (executeFileType) {
    case SSM_P:
      return new ObservationJoinTask(donorSamples, sampleSurrogateSampleIds, resolveControlledFields());
    case SGV_P:
      return new SgvJoinTask(donorSamples, sampleSurrogateSampleIds);
    default:
      return new SecondaryJoinTask(donorSamples, sampleSurrogateSampleIds, executeFileType);
    }
  }

  private List<String> resolveControlledFields() {
    val schema = schemas.get(SSM_P.getId());

    return schema.getFields().stream()
        .filter(filterControlledFields())
        .map(field -> field.getName())
        .collect(Collectors.toImmutableList());
  }

  private static Predicate<? super SubmissionFileField> filterControlledFields() {
    return field -> field.isControlled()
        // This field is excluded from 'controlled' because an observation is created from a 'MASKED' ssm_p
        // which has 'mutated_from_allele' field masked
        && !field.getName().equals(FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_MUTATED_FROM_ALLELE);
  }

  private static Task createPrimaryTask(FileType executeFileType,
      Broadcast<Map<String, Map<String, DonorSample>>> donorSamples) {
    if (executeFileType == FileType.METH_ARRAY_P) {
      return new MethArrayJoinTask(donorSamples);
    }

    return new PrimaryMetaJoinTask(donorSamples, executeFileType);
  }

  private static boolean isPrimaryTask(FileType executeFileType) {
    val taskType = FILE_TYPE_TASK_TYPE.get(executeFileType);

    return taskType == TaskType.PRIMARY;
  }

  private static List<FileType> resolveExecuteFileTypes() {
    val tasksProperty = getProperty(EXECUTE_TASKS_PROPERTY, VALID_TASKS);
    log.info("Requested join tasks: {}", tasksProperty);
    val tasks = COMMA.split(tasksProperty);
    if (Iterables.size(tasks) == 1 && Iterables.contains(tasks, CLINICAL.getId())) {
      return emptyList();
    }

    val result = ImmutableList.<FileType> builder();
    for (val task : tasks) {
      val fileType = resolveTaskFileType(task);
      if (fileType.isPresent()) {
        result.add(fileType.get());
      }
    }

    return result.build();
  }

  private static Optional<FileType> resolveTaskFileType(String task) {
    try {
      val result = FileType.getFileType(task + "_p");

      return Optional.of(result);
    } catch (IllegalArgumentException e) {
      log.warn("Unsupported join task '{}'. Skipping...", task);

      return Optional.empty();
    }
  }

  private static FileType[] getDeleteFileTypes() {
    val result = Sets.newHashSet(ANALYSIS_FILE_TYPES);
    result.add(CLINICAL);
    result.add(OBSERVATION);

    return result.toArray(new FileType[result.size()]);
  }

}
