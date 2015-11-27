package org.icgc.dcc.release.job.document.core;

import static org.icgc.dcc.release.job.document.core.DocumentJob.resolveIndexName;
import static org.icgc.dcc.release.job.document.factory.TransportClientFactory.newTransportClient;
import static org.icgc.dcc.release.job.index.utils.TestUtils.createSnpEffProperties;

import java.io.File;

import lombok.val;

import org.elasticsearch.client.Client;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.job.document.config.DocumentProperties;
import org.icgc.dcc.release.test.job.AbstractJobTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class DocumentJobTest extends AbstractJobTest {

  private static final String PROJECT = "BRCA-UK";
  private static final String ES_URI = "es://localhost:9300";

  /**
   * Class under test.
   */
  DocumentJob job;
  Client esClient;
  String index;

  @Override
  @Before
  public void setUp() {
    super.setUp();
    val properties = new DocumentProperties()
        .setEsUri(ES_URI)
        .setSkipIndexing(true)
        .setOutputDir(new File(workingDir, "output").getAbsolutePath());

    this.job = new DocumentJob(properties, createSnpEffProperties());
    this.index = resolveIndexName(RELEASE_VERSION);
    this.esClient = newTransportClient(ES_URI);
  }

  @After
  public void tearDown() {
    esClient.close();
  }

  @Test
  public void testExecute() {
    given(new File(INPUT_TEST_FIXTURES_DIR));
    job.execute(createJobContext(job.getType(), ImmutableList.of(PROJECT)));

    verifyResult(PROJECT, FileType.OBSERVATION_CENTRIC_INDEX);

    verifyResult(FileType.PROJECT_INDEX);
    verifyResult(FileType.PROJECT_TEXT_INDEX);

    verifyResult(FileType.GENE_SET_INDEX);
    verifyResult(FileType.GENE_SET_TEXT_INDEX);

    verifyResult(FileType.GENE_INDEX);
    verifyResult(FileType.GENE_TEXT_INDEX);
    verifyResult(FileType.GENE_CENTRIC_INDEX);

    verifyResult(FileType.MUTATION_CENTRIC_INDEX);
    verifyResult(FileType.MUTATION_TEXT_INDEX);

    verifyResult(FileType.RELEASE_INDEX);

    verifyResult(PROJECT, FileType.DONOR_CENTRIC_INDEX);
    verifyResult(PROJECT, FileType.DONOR_TEXT_INDEX);
    verifyResult(PROJECT, FileType.DONOR_INDEX);
  }

}
