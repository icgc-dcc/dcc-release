package org.icgc.dcc.release.job.fathmm.core;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;

import lombok.val;

import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.test.job.AbstractJobTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.ImmutableList;

@Ignore("Prepare test data")
public class FathmmJobTest extends AbstractJobTest {

  private static final String PROJECT_NAME = "BRCA-UK";
  private static final String JDBC_URL = format("jdbc:h2:mem;MODE=MySQL;INIT=runscript from '%s'",
      "src/test/resources/sql/fathmm.sql");

  /**
   * Class under test.
   */
  FathmmJob job;

  @Override
  @Before
  public void setUp() {
    super.setUp();
    this.job = new FathmmJob();
    ReflectionTestUtils.setField(job, "jdbcUrl", JDBC_URL);
  }

  @Test
  public void executeTest() {
    given(new File(TEST_FIXTURES_DIR));
    val jobContext = createJobContext(job.getType(), ImmutableList.of(PROJECT_NAME));
    job.execute(jobContext);

    val observations = produces(PROJECT_NAME, FileType.OBSERVATION_FATHMM);
    assertThat(observations).isNotEmpty();
  }

}
