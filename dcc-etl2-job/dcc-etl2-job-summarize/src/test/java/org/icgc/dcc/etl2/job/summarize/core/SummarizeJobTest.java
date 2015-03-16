package org.icgc.dcc.etl2.job.summarize.core;

import static org.assertj.core.api.Assertions.assertThat;
import lombok.val;

import org.icgc.dcc.etl2.core.job.FileType;
import org.icgc.dcc.etl2.test.job.AbstractJobTest;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class SummarizeJobTest extends AbstractJobTest {

  /**
   * Class under test.
   */
  SummarizeJob job;

  @Override
  @Before
  public void setUp() {
    super.setUp();
    this.job = new SummarizeJob();
  }

  @Test
  public void testExecute() {
    val projectName = "PACA-CA";

    given(inputFile(projectName)
        .fileType(FileType.OBSERVATION.name())
        .fileName("observation.json"));

    val jobContext = createJobContext(job.getType(), ImmutableList.of(projectName));
    job.execute(jobContext);

    val results = produces(projectName, "donor-gene-observation-summary");

    assertThat(results).hasSize(1);
    assertThat(results.get(0).get("gene")).isNotNull();
  }

}
