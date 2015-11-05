package org.icgc.dcc.release.job.summarize.task;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;

import lombok.val;

import org.icgc.dcc.common.core.model.FieldNames;
import org.icgc.dcc.common.core.util.stream.Collectors;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.job.JobType;
import org.icgc.dcc.release.core.task.Task;
import org.icgc.dcc.release.test.job.AbstractJobTest;
import org.junit.Before;
import org.junit.Test;

public class MutationSummarizeTaskTest extends AbstractJobTest {

  Task task;

  @Before
  @Override
  public void setUp() {
    super.setUp();
    task = new MutationSummarizeTask();
  }

  @Test
  public void testExecute() {
    given(new File(TEST_FIXTURES_DIR));
    task.execute(createTaskContext(JobType.SUMMARIZE));

    val result = produces(FileType.MUTATION);
    assertThat(result).hasSize(2);

    val mutationIds = result.stream()
        .map(o -> o.get(FieldNames.MUTATION_ID).textValue())
        .collect(Collectors.toImmutableList());
    assertThat(mutationIds).containsOnly("MU1", "MU2");
  }
}
