package org.icgc.dcc.release.job.summarize.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_DATE;
import static org.icgc.dcc.release.test.util.TestJsonNodes.$;

import java.io.File;
import java.util.Optional;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.test.function.DonorJsonComparator;
import org.icgc.dcc.release.test.job.AbstractJobTest;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

@Slf4j
public class SummarizeJobTest extends AbstractJobTest {

  private static final String BRCA_PROJECT_NAME = "BRCA-UK";
  private static final String US_PROJECT_NAME = "ALL-US";
  private static final String DCC_PROJECT_NAME = "TEST0-DCC";

  /**
   * Class under test.
   */
  SummarizeJob job;

  @Override
  @Before
  public void setUp() {
    super.setUp();
    this.job = new SummarizeJob(sparkContext);
  }

  @Test
  public void testExecute() {
    given(new File(INPUT_TEST_FIXTURES_DIR));

    val jobContext = createJobContext(job.getType(), ImmutableList.of(BRCA_PROJECT_NAME, US_PROJECT_NAME,
        DCC_PROJECT_NAME));
    job.execute(jobContext);

    verifyDonor();
    verifyResult(FileType.PROJECT_SUMMARY);
    verifyResult(FileType.GENE_SUMMARY);
    verifyResult(FileType.GENE_SET_SUMMARY);
    assertRelease();

  }

  private void verifyDonor() {
    val donorComaparator = new DonorJsonComparator();
    verifyResult(Optional.of(BRCA_PROJECT_NAME), FileType.DONOR_SUMMARY, donorComaparator);
    verifyResult(Optional.of(DCC_PROJECT_NAME), FileType.DONOR_SUMMARY, donorComaparator);
  }

  private void assertRelease() {
    val releases = produces(FileType.RELEASE_SUMMARY);
    assertThat(releases).hasSize(1);
    log.debug("{}", releases);
    val expectedRelease =
        $("{'_id':'ICGC19-0-2','_release_id':'ICGC19-0-2','name':'ICGC19','number':19,"
            + "'project_count':3,'live_project_count':2,'primary_site_count':2,'live_primary_site_count':2,'donor_count':4,"
            + "'live_donor_count':4,'specimen_count':5,'sample_count':8,'ssm_count':2,'mutated_gene_count':3}");
    val release = releases.get(0);
    val releaseDate = release.remove(RELEASE_DATE).textValue();
    assertThat(releaseDate.length()).isEqualTo(29);
    assertThat(expectedRelease).isEqualTo(release);
  }

}
