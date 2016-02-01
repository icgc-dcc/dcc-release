package org.icgc.dcc.release.job.image.core;

import static java.util.Arrays.asList;

import java.io.File;

import lombok.val;

import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.test.job.AbstractJobTest;
import org.junit.Before;
import org.junit.Test;

public class ImageJobTest extends AbstractJobTest {

  private static final String TCGA_PROJECT = "BLCA-US";
  private static final String NON_TCGA_PROJECT = "PACA-CA";

  /**
   * Class under test.
   */
  ImageJob job;

  @Override
  @Before
  public void setUp() {
    super.setUp();
    this.job = new ImageJob();
  }

  @Test
  public void testExecute() {
    given(new File(INPUT_TEST_FIXTURES_DIR));
    val jobContext = createJobContext(job.getType(), asList(TCGA_PROJECT, NON_TCGA_PROJECT));
    job.execute(jobContext);

    verifyResult(TCGA_PROJECT, FileType.SPECIMEN_SURROGATE_KEY_IMAGE);
    verifyResult(NON_TCGA_PROJECT, FileType.SPECIMEN_SURROGATE_KEY_IMAGE);
  }

}
