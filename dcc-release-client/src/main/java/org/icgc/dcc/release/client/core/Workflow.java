package org.icgc.dcc.release.client.core;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.repeat;
import static org.icgc.dcc.release.core.util.Stopwatches.createStarted;

import java.util.List;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.Path;
import org.icgc.dcc.release.client.mail.Mailer;
import org.icgc.dcc.release.core.job.DefaultJobContext;
import org.icgc.dcc.release.core.job.Job;
import org.icgc.dcc.release.core.job.JobContext;
import org.icgc.dcc.release.core.job.JobSummary;
import org.icgc.dcc.release.core.job.JobType;
import org.icgc.dcc.release.core.submission.SubmissionFileSchema;
import org.icgc.dcc.release.core.submission.SubmissionFileSystem;
import org.icgc.dcc.release.core.submission.SubmissionMetadataService;
import org.icgc.dcc.release.core.task.TaskExecutor;
import org.icgc.dcc.release.core.util.LazyTable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import com.google.common.collect.Table;

@Slf4j
@Lazy
@Service
@RequiredArgsConstructor(onConstructor = @__({ @Autowired }))
public class Workflow {

  /**
   * Submission dependencies.
   */
  @NonNull
  private final SubmissionMetadataService submissionMetadata;
  @NonNull
  private final SubmissionFileSystem submissionFileSystem;
  @NonNull
  private final TaskExecutor taskExecutor;
  @NonNull
  private final Mailer mailer;

  /**
   * Job dependencies.
   */
  @NonNull
  private final List<Job> jobs;

  public void execute(@NonNull WorkflowContext workflowContext) {
    val watch = createStarted();
    log.info("Executing workflow...");

    val submissionFiles = resolveSubmissionFiles(workflowContext);

    executeJobs(submissionFiles, workflowContext);

    log.info("Finished executing workflow in {}", watch);
  }

  private void executeJobs(Table<String, String, List<Path>> submissionFiles, WorkflowContext workflowContext) {
    for (val jobType : JobType.getTopologicalSortOrder()) {
      // Filter
      val included = workflowContext.isIncluded(jobType);
      if (!included) {
        continue;
      }

      // Resolve
      val job = findJob(jobType);
      val jobContext = createJobContext(jobType, workflowContext, submissionFiles);

      val watch = createStarted();
      log.info("{}", repeat("-", 100));
      log.info("Executing job '{}'...", jobType);
      log.info("{}", repeat("-", 100));

      // Execute
      try {
        job.execute(jobContext);
      } catch (Exception e) {
        log.warn("Emailing '{}' failed job summary...", jobType);
        val summary = new JobSummary(jobType, watch);
        mailer.sendFailedJob(summary, e);

        throw e;
      }

      log.info("{}", repeat("-", 100));
      log.info("Finished executing job '{}' in {}", jobType, watch);
      log.info("{}", repeat("-", 100));

      // Notify
      log.info("Emailing '{}' job summary...", jobType);
      val summary = new JobSummary(jobType, watch);
      mailer.sendJobSummary(summary);
    }
  }

  private Table<String, String, List<Path>> resolveSubmissionFiles(WorkflowContext workflowContext) {
    return new LazyTable<String, String, List<Path>>(() -> {
      List<SubmissionFileSchema> metadata = submissionMetadata.getMetadata();

      return submissionFileSystem.getFiles(workflowContext.getReleaseDir(), workflowContext.getProjectNames(),
          metadata);
    });
  }

  private JobContext createJobContext(JobType type, WorkflowContext workflowContext,
      Table<String, String, List<Path>> submissionFiles) {
    return new DefaultJobContext(
        type,
        workflowContext.getReleaseName(),
        workflowContext.getProjectNames(),
        workflowContext.getReleaseDir(),
        workflowContext.getWorkingDir(),
        submissionFiles,
        taskExecutor,
        workflowContext.isCompressOutput());
  }

  private Job findJob(JobType jobType) {
    val result = jobs.stream().filter(job -> job.getType() == jobType).findFirst();
    checkArgument(result.isPresent(), "Job type '%s' unavailable in '%s'", jobType, jobs);

    return result.get();
  }

}
