package org.icgc.dcc.release.job.fi.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;

import lombok.val;

import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.test.job.AbstractJobTest;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class FunctionalImpactJobTest extends AbstractJobTest {

  private static final String PROJECT_NAME = "BRCA-UK";

  /**
   * Class under test.
   */
  FunctionalImpactJob job;

  @Override
  @Before
  public void setUp() {
    super.setUp();
    this.job = new FunctionalImpactJob();
  }

  @Test
  public void executeTest() {
    given(new File(TEST_FIXTURES_DIR));
    val jobContext = createJobContext(job.getType(), ImmutableList.of(PROJECT_NAME));
    job.execute(jobContext);

    val observations = produces(PROJECT_NAME, FileType.OBSERVATION_FI);
    assertThat(observations).isNotEmpty();
  }

}
