package org.icgc.dcc.etl2.job.orphan.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.List;

import lombok.val;

import org.icgc.dcc.etl2.core.job.FileType;
import org.icgc.dcc.etl2.test.job.AbstractJobTest;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

public class OrphanJobTest extends AbstractJobTest {

  /**
   * Constants.
   */
  private static final String TEST_FIXTURES_DIR = "src/test/resources/fixtures";
  private static final String INPUT_DIR = TEST_FIXTURES_DIR + "/staging";

  /**
   * Class under test.
   */
  OrphanJob job;

  @Override
  @Before
  public void setUp() {
    super.setUp();
    this.job = new OrphanJob();
  }

  @Test
  public void testExecute() {
    List<String> projectNames = ImmutableList.of("All-US", "CMDI-UK", "EOPC-DE", "LAML-KR", "PRAD-CA");

    given(new File(INPUT_DIR));

    val jobContext = createJobContext(job.getType(), projectNames);
    job.execute(jobContext);

    for (String projectName : projectNames) {
      if (projectName.equals("LAML-KR")) {
        val donorResults = produces(projectName, FileType.DONOR_ORPHANED);
        assertThat(orphanCount(donorResults)).isZero();

        val specimenResults = produces(projectName, FileType.SPECIMEN_ORPHANED);
        assertThat(orphanCount(specimenResults)).isZero();

        val sampleResults = produces(projectName, FileType.SAMPLE_ORPHANED);
        assertThat(orphanCount(sampleResults)).isZero();
      }

      if (projectName.equals("ALL-US")) {
        val donorResults = produces(projectName, FileType.DONOR_ORPHANED);
        assertThat(orphanCount(donorResults)).isEqualTo(213);

        val specimenResults = produces(projectName, FileType.SPECIMEN_ORPHANED);
        assertThat(orphanCount(specimenResults)).isEqualTo(535);

        val sampleResults = produces(projectName, FileType.SPECIMEN_ORPHANED);
        assertThat(orphanCount(sampleResults)).isEqualTo(899);
      }
    }
  }

  private long orphanCount(List<ObjectNode> rows) {
    return rows
        .stream()
        .filter(row -> row.get("orphaned").asText().equals("true"))
        .count();
  }
}
