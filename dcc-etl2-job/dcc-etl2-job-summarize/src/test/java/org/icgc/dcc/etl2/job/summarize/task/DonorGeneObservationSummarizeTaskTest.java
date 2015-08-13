package org.icgc.dcc.etl2.job.summarize.task;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.core.model.FeatureTypes.FeatureType.SSM_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_GENES;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_GENE_GENE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_GENE_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SUMMARY_AFFECTED_GENE_COUNT;
import static org.icgc.dcc.etl2.core.util.FieldNames.SummarizeFieldNames.FAKE_GENE_ID;
import static org.icgc.dcc.etl2.core.util.ObjectNodes.textValue;

import java.io.File;
import java.util.List;

import lombok.val;

import org.icgc.dcc.etl2.core.job.JobType;
import org.icgc.dcc.etl2.test.job.AbstractJobTest;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.ImmutableList;

public class DonorGeneObservationSummarizeTaskTest extends AbstractJobTest {

  private static final List<String> GENE_IDS = ImmutableList.of("GID1", "GID2", "GID3", FAKE_GENE_ID);

  DonorGeneObservationSummarizeTask task = new DonorGeneObservationSummarizeTask();

  @Before
  @Override
  public void setUp() {
    super.setUp();
  }

  @Test
  public void testProcess() throws Exception {
    given(new File(TEST_FIXTURES_DIR));
    task.execute(createTaskContext(JobType.SUMMARIZE));
    val result = task.getSummary().collectAsMap();

    assertThat(result).hasSize(1);
    val donorSummary = result.get("DO001");
    assertGenes(donorSummary.get(DONOR_GENES));
    assertSummary(donorSummary.get(DONOR_GENE_SUMMARY));
  }

  private static void assertGenes(JsonNode genes) {
    val genesArray = (ArrayNode) genes;
    assertThat(genesArray).hasSize(4);
    for (val gene : genesArray) {
      val geneId = textValue(gene, DONOR_GENE_GENE_ID);
      assertThat(GENE_IDS).contains(geneId);
      val genesCount = gene.get(DONOR_GENE_SUMMARY).get(SSM_TYPE.getSummaryFieldName()).asInt();
      if (geneId.equals("GID2")) {
        assertThat(genesCount).isEqualTo(2);
      } else {
        assertThat(genesCount).isEqualTo(1);
      }
    }
  }

  private static void assertSummary(JsonNode summary) {
    assertThat(summary.get(DONOR_SUMMARY_AFFECTED_GENE_COUNT).asInt()).isEqualTo(4);
  }

}
