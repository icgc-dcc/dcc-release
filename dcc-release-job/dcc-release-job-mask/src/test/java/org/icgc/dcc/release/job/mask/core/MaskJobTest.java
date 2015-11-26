package org.icgc.dcc.release.job.mask.core;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.core.model.FieldNames.NormalizerFieldNames.NORMALIZER_OBSERVATION_ID;
import static org.icgc.dcc.release.test.util.TestJsonNodes.$;

import java.io.File;
import java.util.UUID;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.job.Job;
import org.icgc.dcc.release.test.job.AbstractJobTest;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;

@Slf4j
public class MaskJobTest extends AbstractJobTest {

  private static final String PROJECT_NAME = "BRCA-UK";

  Job job;

  @Before
  @Override
  public void setUp() {
    super.setUp();
    this.job = new MaskJob();
  }

  @Test
  public void executeTest() {
    given(new File(TEST_FIXTURES_DIR));
    val jobContext = createJobContext(job.getType(), asList(PROJECT_NAME));
    job.execute(jobContext);

    val result = produces(PROJECT_NAME, FileType.SSM_P_MASKED);
    log.debug("Results: {}", result);
    val controlledMutation = result.get(0);
    val controlledObservationId = controlledMutation.get(NORMALIZER_OBSERVATION_ID).textValue();
    val expectedControlled =
        $("{reference_genome_allele:'G',control_genotype:'G/T',mutated_from_allele:'T',mutated_to_allele:"
            + "'A',tumour_genotype:'G/A',marking:'CONTROLLED',mutation:'T>A'}");
    assertControlled(controlledMutation, expectedControlled);

    val maskedMutation = result.get(1);
    val maskedObservationId = maskedMutation.get(NORMALIZER_OBSERVATION_ID).textValue();
    assertThat(maskedObservationId).isNotEqualTo(controlledObservationId);

    val expectedMasked =
        $("{reference_genome_allele:'G',control_genotype:null,mutated_from_allele:'G',mutated_to_allele:"
            + "'A',tumour_genotype:null,marking:'MASKED',mutation:'G>A'}");
    assertControlled(maskedMutation, expectedMasked);
  }

  private static void assertControlled(ObjectNode mutation, ObjectNode expected) {
    val controlledObservationId = mutation.remove(NORMALIZER_OBSERVATION_ID).textValue();
    // Will throw exception if it's not UUID
    UUID.fromString(controlledObservationId);
    assertThat(mutation).isEqualTo(expected);
  }

}
