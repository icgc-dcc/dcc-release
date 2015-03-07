package org.icgc.dcc.etl2.workflow.core;

import static com.google.common.base.Preconditions.checkArgument;
import static org.icgc.dcc.etl2.core.util.Stopwatches.createStarted;

import java.util.List;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.Path;
import org.icgc.dcc.etl2.core.job.DefaultJobContext;
import org.icgc.dcc.etl2.core.job.Job;
import org.icgc.dcc.etl2.core.job.JobContext;
import org.icgc.dcc.etl2.core.job.JobSummary;
import org.icgc.dcc.etl2.core.job.JobType;
import org.icgc.dcc.etl2.core.submission.SubmissionFileSystem;
import org.icgc.dcc.etl2.core.submission.SubmissionMetadataRepository;
import org.icgc.dcc.etl2.workflow.mail.Mailer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.common.collect.Table;

@Slf4j
@Service
@RequiredArgsConstructor(onConstructor = @__({ @Autowired }))
public class Workflow {

  /**
   * Submission dependencies.
   */
  @NonNull
  private final SubmissionMetadataRepository submissionMetadata;
  @NonNull
  private final SubmissionFileSystem submissionFileSystem;
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

    val submissionFiles = resolveFiles(workflowContext);
    val jobContext = createJobContext(workflowContext, submissionFiles);

    executeJobs(workflowContext, jobContext);

    log.info("Finished executing workflowContext in {}", watch);
  }

  private void executeJobs(WorkflowContext workflowContext, JobContext jobContext) {
    for (val jobType : JobType.getTopologicalSortOrder()) {
      val included = workflowContext.isIncluded(jobType);
      if (!included) {
        continue;
      }

      val job = findJob(jobType);

      val watch = createStarted();
      log.info("Executing job '{}'...", jobType);
      job.execute(jobContext);
      log.info("Finished executing job '{}' in {}", jobType, watch);

      log.info("Emailing '{}' job summary...", jobType);
      val summary = new JobSummary(jobType, watch);
      mailer.sendJobSummary(summary);
    }
  }

  private Table<String, String, List<Path>> resolveFiles(WorkflowContext workflowContext) {
    val schemas = submissionMetadata.getSchemas();

    return submissionFileSystem.getFiles(workflowContext.getReleaseDir(), workflowContext.getProjectNames(), schemas);
  }

  private JobContext createJobContext(WorkflowContext workflowContext, Table<String, String, List<Path>> submissionFiles) {
    return new DefaultJobContext(
        workflowContext.getReleaseDir(),
        workflowContext.getProjectNames(),
        workflowContext.getReleaseDir(),
        workflowContext.getWorkingDir(),
        submissionFiles);
  }

  private Job findJob(JobType jobType) {
    val result = jobs.stream().filter(job -> job.getType() == jobType).findFirst();
    checkArgument(result.isPresent(), "Job type '%s' unavailable in '%s'", jobType, jobs);

    return result.get();
  }

}
