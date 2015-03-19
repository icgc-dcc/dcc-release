package org.icgc.dcc.etl2.job.image.core;

import static com.google.common.collect.ImmutableList.of;
import static org.assertj.core.api.Assertions.assertThat;
import lombok.val;

import org.icgc.dcc.etl2.core.job.FileType;
import org.icgc.dcc.etl2.test.job.AbstractJobTest;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class ImageJobTest extends AbstractJobTest {

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
    val projectName = "PACA-CA";

    given(inputFile(projectName)
        .fileType(FileType.SPECIMEN_SURROGATE_KEY)
        .rows(of(row("{specimen_id: 1}"))));

    val jobContext = createJobContext(job.getType(), ImmutableList.of(projectName));
    job.execute(jobContext);

    val results = produces(projectName, FileType.SPECIMEN_SURROGATE_KEY_IMAGE);

    assertThat(results).hasSize(1);
    assertThat(results.get(0)).isEqualTo(row("{specimen_id: 1, digital_image_of_stained_section: null}"));
  }

}
