package org.icgc.dcc.release.job.document.core;

import static org.icgc.dcc.release.job.index.utils.TestUtils.createSnpEffProperties;

import java.io.File;

import lombok.val;

import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.job.document.config.DocumentProperties;
import org.icgc.dcc.release.test.job.AbstractJobTest;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class DocumentJobTest extends AbstractJobTest {

  private static final String PROJECT = "BRCA-UK";

  /**
   * Class under test.
   */
  DocumentJob job;

  @Override
  @Before
  public void setUp() {
    super.setUp();
    val properties = new DocumentProperties();
    this.job = new DocumentJob(properties, createSnpEffProperties(), sparkContext);
  }

  @Test
  public void testExecute() {
    given(new File(INPUT_TEST_FIXTURES_DIR));
    job.execute(createJobContext(job.getType(), ImmutableList.of(PROJECT)));

    verifyResult(FileType.DRUG_TEXT_DOCUMENT);
    verifyResult(FileType.DRUG_CENTRIC_DOCUMENT);
    verifyResult(FileType.DIAGRAM_DOCUMENT);
    verifyResult(PROJECT, FileType.OBSERVATION_CENTRIC_DOCUMENT);
    verifyResult(FileType.PROJECT_DOCUMENT);
    verifyResult(FileType.PROJECT_TEXT_DOCUMENT);

    verifyResult(FileType.GENE_SET_DOCUMENT);
    verifyResult(FileType.GENE_SET_TEXT_DOCUMENT);

    verifyResult(FileType.GENE_DOCUMENT);
    verifyResult(FileType.GENE_TEXT_DOCUMENT);
    verifyResult(FileType.GENE_CENTRIC_DOCUMENT);

    verifyResult(FileType.MUTATION_CENTRIC_DOCUMENT);
    verifyResult(FileType.MUTATION_TEXT_DOCUMENT);

    verifyResult(FileType.RELEASE_DOCUMENT);

    verifyResult(PROJECT, FileType.DONOR_CENTRIC_DOCUMENT);
    verifyResult(PROJECT, FileType.DONOR_TEXT_DOCUMENT);
    verifyResult(PROJECT, FileType.DONOR_DOCUMENT);
  }

}
