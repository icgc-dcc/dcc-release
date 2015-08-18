package org.icgc.dcc.etl2.job.summarize.core;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.core.model.FeatureTypes.FeatureType.SSM_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.AVAILABLE_DATA_TYPES;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_GENES;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_GENE_GENE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_GENE_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_ID;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SUMMARY_AFFECTED_GENE_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SUMMARY_AGE_AT_DIAGNOSIS_GROUP;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SUMMARY_EXPERIMENTAL_ANALYSIS;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SUMMARY_EXPERIMENTAL_ANALYSIS_SAMPLE_COUNTS;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SUMMARY_STATE;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SUMMARY_STUDIES;
import static org.icgc.dcc.common.core.model.FieldNames.SEQUENCE_DATA_REPOSITORY;
import static org.icgc.dcc.common.core.util.Jackson.asArrayNode;
import static org.icgc.dcc.common.core.util.Jackson.asObjectNode;
import static org.icgc.dcc.etl2.core.util.FeatureTypes.getFeatureTypes;
import static org.icgc.dcc.etl2.core.util.FieldNames.SummarizeFieldNames.FAKE_GENE_ID;
import static org.icgc.dcc.etl2.core.util.ObjectNodes.textValue;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import lombok.val;

import org.icgc.dcc.common.core.model.FeatureTypes.FeatureType;
import org.icgc.dcc.common.core.util.stream.Streams;
import org.icgc.dcc.etl2.core.job.FileType;
import org.icgc.dcc.etl2.test.job.AbstractJobTest;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

public class SummarizeJobTest extends AbstractJobTest {

  private static final List<String> DO1_GENE_IDS = ImmutableList.of("GID1", "GID2", "GID3", FAKE_GENE_ID);
  private static final List<String> DO1_REPOS = ImmutableList.of("CGHub", "TCGA", "EGA");
  private static final List<String> DO1_EXPERIMENTAL_ANALYSIS_PERFORMED = ImmutableList.of(
      "WGS", "miRNA-Seq", "WXS", "Bisulfite-Seq", "RNA-Seq");
  private static final List<FeatureType> DO2_AVAILABLE_DATA_TYPES = Collections.emptyList();
  private static final List<FeatureType> DO1_AVAILABLE_DATA_TYPES = ImmutableList.of(
      FeatureType.MIRNA_SEQ_TYPE,
      FeatureType.METH_ARRAY_TYPE,
      FeatureType.EXP_ARRAY_TYPE,
      FeatureType.JCN_TYPE,
      FeatureType.METH_SEQ_TYPE,
      FeatureType.PEXP_TYPE,
      FeatureType.STSM_TYPE,
      FeatureType.SGV_TYPE,
      FeatureType.CNSM_TYPE,
      FeatureType.EXP_SEQ_TYPE,
      FeatureType.SSM_TYPE);

  /**
   * Class under test.
   */
  SummarizeJob job;

  @Override
  @Before
  public void setUp() {
    super.setUp();
    this.job = new SummarizeJob();
  }

  @Test
  public void testExecute() {
    val projectName = "BRCA-UK";
    given(new File(TEST_FIXTURES_DIR));

    val jobContext = createJobContext(job.getType(), ImmutableList.of(projectName));
    job.execute(jobContext);

    val results = produces(projectName, FileType.DONOR_GENE_OBSERVATION_SUMMARY);

    assertThat(results).hasSize(2);
    for (val donor : results) {
      if (textValue(donor, DONOR_ID).equals("DO001")) {
        assertGenes(donor.get(DONOR_GENES));
        assertSummaryDO1(donor.get(DONOR_SUMMARY));
      } else {
        assertThat(donor.path(DONOR_GENES).isMissingNode()).isTrue();
        assertSummaryDO2(donor.get(DONOR_SUMMARY));
      }
    }
  }

  private static void assertSummaryDO2(JsonNode summary) {
    assertArray(summary.get(SEQUENCE_DATA_REPOSITORY), Collections.emptyList());
    assertArray(summary.get(DONOR_SUMMARY_STUDIES), Arrays.asList("PCAWG"));
    assertArray(summary.get(DONOR_SUMMARY_EXPERIMENTAL_ANALYSIS), Collections.emptyList());

    // TODO: If each donor has at least one mutation this condition is false
    // assertThat(summary.get(DONOR_SUMMARY_AFFECTED_GENE_COUNT).asInt()).isEqualTo(0);
    assertThat(asObjectNode(summary.get(DONOR_SUMMARY_EXPERIMENTAL_ANALYSIS_SAMPLE_COUNTS))).isEmpty();
    assertThat(textValue(summary, DONOR_SUMMARY_AGE_AT_DIAGNOSIS_GROUP)).isEqualTo("40 - 49");
    assertArray(summary.get(AVAILABLE_DATA_TYPES), Collections.singletonList(FeatureType.SSM_TYPE.getId()));
    assertFeatureTypes(asObjectNode(summary), DO2_AVAILABLE_DATA_TYPES);
    assertThat(textValue(summary, DONOR_SUMMARY_STATE)).isEqualTo("live");
    assertThat(summary.get(FeatureType.SSM_TYPE.getSummaryFieldName()).asInt()).isEqualTo(1);
  }

  private static void assertGenes(JsonNode genes) {
    val genesArray = asArrayNode(genes);
    assertThat(genesArray).hasSize(4);
    for (val gene : genesArray) {
      val geneId = textValue(gene, DONOR_GENE_GENE_ID);
      assertThat(DO1_GENE_IDS).contains(geneId);
      val genesCount = gene.get(DONOR_GENE_SUMMARY).get(SSM_TYPE.getSummaryFieldName()).asInt();
      if (geneId.equals("GID2")) {
        assertThat(genesCount).isEqualTo(2);
      } else {
        assertThat(genesCount).isEqualTo(1);
      }
    }
  }

  private static void assertSummaryDO1(JsonNode summary) {
    assertThat(summary.get(DONOR_SUMMARY_AFFECTED_GENE_COUNT).asInt()).isEqualTo(4);
    assertArray(summary.get(SEQUENCE_DATA_REPOSITORY), DO1_REPOS);
    assertArray(summary.get(DONOR_SUMMARY_STUDIES), Arrays.asList("PCAWG", "STUDY2"));
    assertArray(summary.get(DONOR_SUMMARY_EXPERIMENTAL_ANALYSIS), DO1_EXPERIMENTAL_ANALYSIS_PERFORMED);
    assertExperimentalAnalysisCounts(asObjectNode(summary.get(DONOR_SUMMARY_EXPERIMENTAL_ANALYSIS_SAMPLE_COUNTS)));
    assertThat(textValue(summary, DONOR_SUMMARY_AGE_AT_DIAGNOSIS_GROUP)).isEqualTo("40 - 49");
    assertArray(summary.get(AVAILABLE_DATA_TYPES), resolveFeatureTypeIds(DO1_AVAILABLE_DATA_TYPES));
    assertFeatureTypes(asObjectNode(summary), DO1_AVAILABLE_DATA_TYPES);
    assertThat(textValue(summary, DONOR_SUMMARY_STATE)).isEqualTo("live");
    assertThat(summary.get(FeatureType.SSM_TYPE.getSummaryFieldName()).asInt()).isEqualTo(2);
  }

  private static void assertFeatureTypes(ObjectNode summary, List<FeatureType> availableFeatureTypes) {
    for (val featureType : getFeatureTypes()) {
      if (featureType == FeatureType.SSM_TYPE) {
        // Asserted separately
        continue;
      }

      val featureTypeSummaryFieldValue = summary.get(featureType.getSummaryFieldName()).asBoolean();
      if (availableFeatureTypes.contains(featureType)) {
        assertThat(featureTypeSummaryFieldValue).isTrue();
      } else {
        assertThat(featureTypeSummaryFieldValue).isFalse();
      }
    }
  }

  private static List<String> resolveFeatureTypeIds(List<FeatureType> featureTypes) {
    return featureTypes.stream()
        .map(ft -> ft.getId())
        .collect(toList());
  }

  private static void assertExperimentalAnalysisCounts(ObjectNode jsonNode) {
    assertThat(jsonNode).hasSize(5);
    assertThat(jsonNode.get("WGS").asInt()).isEqualTo(1);
    assertThat(jsonNode.get("miRNA-Seq").asInt()).isEqualTo(1);
    assertThat(jsonNode.get("Bisulfite-Seq").asInt()).isEqualTo(1);
    assertThat(jsonNode.get("RNA-Seq").asInt()).isEqualTo(2);
    assertThat(jsonNode.get("WXS").asInt()).isEqualTo(3);
  }

  private static void assertArray(JsonNode resultNode, Iterable<String> valuesList) {
    val array = Streams.stream(asArrayNode(resultNode))
        .map(jn -> jn.textValue())
        .collect(Collectors.toList());

    assertThat(valuesList).containsOnlyElementsOf(array);
  }

}
