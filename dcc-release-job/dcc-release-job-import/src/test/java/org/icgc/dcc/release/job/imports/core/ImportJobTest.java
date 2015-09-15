package org.icgc.dcc.release.job.imports.core;

import lombok.val;

import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.job.imports.config.MongoProperties;
import org.icgc.dcc.release.job.imports.core.ImportJob;
import org.icgc.dcc.release.test.job.AbstractJobTest;
import org.junit.Before;
import org.junit.Test;

public class ImportJobTest extends AbstractJobTest {

  /**
   * Class under test.
   */
  ImportJob job;

  @Override
  @Before
  public void setUp() {
    super.setUp();
    this.job = new ImportJob(new MongoProperties());
  }

  @Test
  public void testExecute() {
    job.execute(createJobContext(job.getType()));

    val results = producesFile(FileType.GENE);
    for (val gene : results) {
      System.out.println(gene);
    }
  }

}
