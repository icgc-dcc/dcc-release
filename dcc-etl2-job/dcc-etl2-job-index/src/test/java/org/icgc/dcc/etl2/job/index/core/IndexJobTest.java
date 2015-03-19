package org.icgc.dcc.etl2.job.index.core;

import java.io.File;

import lombok.val;

import org.icgc.dcc.etl2.core.job.FileType;
import org.icgc.dcc.etl2.job.index.config.IndexProperties;
import org.icgc.dcc.etl2.test.job.AbstractJobTest;
import org.junit.Before;
import org.junit.Test;

public class IndexJobTest extends AbstractJobTest {

  /**
   * Class under test.
   */
  IndexJob job;

  @Override
  @Before
  public void setUp() {
    super.setUp();
    val properties = new IndexProperties()
        .setIndexName("index")
        .setEsUri("es://localhost:9300")
        .setOutputDir(new File(workingDir, "output").getAbsolutePath());

    this.job = new IndexJob(properties);
  }

  @Test
  public void testExecute() {
    given(inputFile()
        .fileType(FileType.RELEASE)
        .fileName("working/release"));
    given(inputFile()
        .fileType(FileType.PROJECT)
        .fileName("working/project"));
    given(inputFile()
        .fileType(FileType.GENE)
        .fileName("working/gene"));

    job.execute(createJobContext(job.getType()));
  }

}
