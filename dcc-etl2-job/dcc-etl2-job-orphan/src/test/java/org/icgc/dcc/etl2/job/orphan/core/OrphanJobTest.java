package org.icgc.dcc.etl2.job.orphan.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.List;

import lombok.val;

import org.icgc.dcc.etl2.test.job.AbstractJobTest;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class OrphanJobTest extends AbstractJobTest {

  /**
   * Constants.
   */
  private static final File TEST_FIXTURES_DIR = new File("src/test/resources/fixtures");

  /**
   * Class under test.
   */
  OrphanJob job;

  @Override
  @Before
  public void setUp() {
    super.setUp();
    this.job = new OrphanJob();
    // this.workingDir = new File(super.TEST_FIXTURES_DIR + "/staging");
  }

  @Test
  public void testExecute() {
    List<String> projectNames = ImmutableList.of("All-US", "CMDI-UK", "EOPC-DE", "LAML-KR", "PRAD-CA");

    given(new File(TEST_FIXTURES_DIR + "/staging"));

    val jobContext = createJobContext(job.getType(), projectNames);
    job.execute(jobContext);

    for (String projectName : projectNames) {
      val results = produces(projectName, "donor_orphaned");
      assertThat(results.get(0).get("orphaned")).isNotNull();
    }

  }
}
