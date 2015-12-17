package org.icgc.dcc.release.job.join.task;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.job.JobType;
import org.icgc.dcc.release.core.task.TaskContext;
import org.icgc.dcc.release.test.job.AbstractJobTest;
import org.junit.Before;
import org.junit.Test;

@Slf4j
public class SgvJoinTaskTest extends AbstractJobTest {

  private static final String PROJECT_NAME = "BRCA-UK";

  SgvJoinTask task;

  TaskContext taskContext;

  @Before
  @Override
  public void setUp() {
    super.setUp();
    given(new File(INPUT_TEST_FIXTURES_DIR));
    taskContext = createTaskContext(JobType.JOIN, PROJECT_NAME);
    val sparkContext = taskContext.getSparkContext();

    task = new SgvJoinTask(sparkContext.broadcast(emptyMap()), sparkContext.broadcast(emptyMap()));
  }

  @Test
  public void testExecute() throws Exception {
    task.execute(createTaskContext(JobType.JOIN, PROJECT_NAME));
    val sgv = produces(PROJECT_NAME, FileType.SGV);
    log.info("SGV: {}", sgv);
    assertThat(sgv).isNotEmpty();
  }

}
