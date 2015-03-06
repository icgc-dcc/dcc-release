package org.icgc.dcc.etl2.job.image.core;

import static com.google.common.collect.ImmutableList.of;
import static org.assertj.core.api.Assertions.assertThat;
import lombok.val;

import org.icgc.dcc.etl2.job.image.core.ImageJob;
import org.icgc.dcc.etl2.test.job.AbstractJobTest;
import org.junit.Before;
import org.junit.Test;

public class ImageJobTest extends AbstractJobTest {

  /**
   * Class under test.
   */
  ImageJob job;

  @Override
  @Before
  public void setUp() {
    super.setUp();
    this.job = new ImageJob(taskExecutor);
  }

  @Test
  public void testExecute() {
    val projectName = "PACA-CA";

    given(inputFile(projectName)
        .fileType("specimen_surrogate_key")
        .rows(of(row("{specimen_id: 1}"))));

    job.execute(createContext(projectName));

    val results = produces(projectName, "specimen_surrogate_key_image");

    assertThat(results).hasSize(1);
    assertThat(results.get(0)).isEqualTo(row("{specimen_id: 1, digital_image_of_stained_section: null}"));
  }

}
