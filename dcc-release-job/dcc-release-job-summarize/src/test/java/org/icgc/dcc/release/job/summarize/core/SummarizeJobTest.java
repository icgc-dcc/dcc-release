package org.icgc.dcc.release.job.summarize.core;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.core.model.FeatureTypes.FeatureType.SGV_TYPE;
import static org.icgc.dcc.common.core.model.FeatureTypes.FeatureType.SSM_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.AFFECTED_DONOR_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.AVAILABLE_DATA_TYPES;
import static org.icgc.dcc.common.core.model.FieldNames.AVAILABLE_EXPERIMENTAL_ANALYSIS_PERFORMED;
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
import static org.icgc.dcc.common.core.model.FieldNames.GENE_DONORS;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_DONOR_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_PROJECTS;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_PROJECT_PROJECT_ID;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_PROJECT_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCE_TYPES;
import static org.icgc.dcc.common.core.model.FieldNames.PROJECT_ID;
import static org.icgc.dcc.common.core.model.FieldNames.PROJECT_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.PROJECT_SUMMARY_REPOSITORY;
import static org.icgc.dcc.common.core.model.FieldNames.PROJECT_SUMMARY_STATE;
import static org.icgc.dcc.common.core.model.FieldNames.RELEASE_DATE;
import static org.icgc.dcc.common.core.model.FieldNames.SEQUENCE_DATA_REPOSITORY;
import static org.icgc.dcc.common.core.model.FieldNames.TOTAL_DONOR_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.TOTAL_LIVE_DONOR_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.TOTAL_SAMPLE_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.TOTAL_SPECIMEN_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.getTestedTypeCountFieldName;
import static org.icgc.dcc.common.core.util.Jackson.asArrayNode;
import static org.icgc.dcc.common.core.util.Jackson.asObjectNode;
import static org.icgc.dcc.release.core.util.FeatureTypes.createFeatureTypeSummaryValue;
import static org.icgc.dcc.release.core.util.FeatureTypes.getFeatureTypes;
import static org.icgc.dcc.release.core.util.FieldNames.SummarizeFieldNames.FAKE_GENE_ID;
import static org.icgc.dcc.release.core.util.ObjectNodes.textValue;
import static org.icgc.dcc.release.test.util.TestJsonNodes.$;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.core.model.FeatureTypes.FeatureType;
import org.icgc.dcc.common.core.util.stream.Streams;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.job.summarize.core.SummarizeJob;
import org.icgc.dcc.release.test.job.AbstractJobTest;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

@Slf4j
public class SummarizeJobTest extends AbstractJobTest {

  private static final String BRCA_PROJECT_NAME = "BRCA-UK";
  private static final String US_PROJECT_NAME = "ALL-US";
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
    given(new File(TEST_FIXTURES_DIR));

    val jobContext = createJobContext(job.getType(), ImmutableList.of(BRCA_PROJECT_NAME, US_PROJECT_NAME));
    job.execute(jobContext);

    assertDonorSummary(BRCA_PROJECT_NAME);
    assertProjects();
    assertGenes();
    assertObservations();
    assertRelease();

  }

  private void assertRelease() {
    val releases = produces(FileType.RELEASE_SUMMARY);
    assertThat(releases).hasSize(1);
    log.debug("{}", releases);
    val expectedRelease = $("{_id:'ICGC19-0-2',_release_id:'ICGC19-0-2',name:'ICGC19',number:19,project_count:2,"
        + "live_project_count:1,primary_site_count:2,live_primary_site_count:1,donor_count:2,live_donor_count:0,"
        + "specimen_count:2,sample_count:3,ssm_count:0,mutated_gene_count:0}");
    val release = releases.get(0);
    val releaseDate = release.remove(RELEASE_DATE).textValue();
    assertThat(releaseDate.length()).isEqualTo(29);
    assertThat(expectedRelease).isEqualTo(release);
  }

  private void assertObservations() {
    val observations = produces(BRCA_PROJECT_NAME, FileType.OBSERVATION_SUMMARY);
    assertThat(observations).hasSize(3);
    assertObservation(observations.get(0), asList("downstream_gene_variant", "gene_variant"));
    assertObservation(observations.get(1), asList("downstream_gene_variant", "synonymous_variant"));
    assertObservation(observations.get(2), asList("downstream_gene_variant", "inframe_insertion"));
  }

  private static void assertObservation(ObjectNode observation, List<String> expectedConsequenceTypes) {
    val consequenceTypes = observation.get(OBSERVATION_CONSEQUENCE_TYPES);
    assertArray(consequenceTypes, expectedConsequenceTypes);
  }

  private void assertGenes() {
    val genes = produces(FileType.GENE_SUMMARY);
    assertThat(genes).hasSize(3);
    log.debug("Gene Summary - {}", genes);
    for (val gene : genes) {
      val geneId = textValue(gene, GENE_ID);
      switch (geneId) {
      case "G1":
        assertG1gene(gene);
        break;
      case "G2":
        assertG2gene(gene);
        break;
      case "G3":
        assertG3gene(gene);
        break;
      }
    }
  }

  private static void assertG1gene(ObjectNode gene) {
    // Donors
    val donors = getGeneDonors(gene);
    assertThat(donors).hasSize(4);
    for (val donor : donors) {
      val donorId = textValue(donor, DONOR_ID);
      switch (donorId) {
      case "DO001":
        assertGeneDonor(donor, SSM_TYPE, 1);
        break;
      case "DO002":
        assertGeneDonor(donor, SGV_TYPE, 1);
        break;
      case "DO003":
        assertGeneDonor(donor, SSM_TYPE, 1);
        break;
      case "DO004":
        assertGeneDonor(donor, SGV_TYPE, 1);
        break;
      }
    }

    // Projects
    val projects = getGeneProjects(gene);
    assertThat(projects).hasSize(2);
    for (val project : projects) {
      val projectId = textValue(project, GENE_PROJECT_PROJECT_ID);
      assertThat(asList(BRCA_PROJECT_NAME, US_PROJECT_NAME)).contains(projectId);
      assertGeneProject(asObjectNode(project.get(GENE_PROJECT_SUMMARY)), 2, asList(SSM_TYPE, SGV_TYPE));
    }
  }

  public static void assertGeneProject(ObjectNode projectSummary, int expectedDonorCount, List<FeatureType> dataTypes) {
    val donorCount = projectSummary.get(AFFECTED_DONOR_COUNT).asInt();
    assertThat(donorCount).isEqualTo(expectedDonorCount);
    assertArray(projectSummary.get(AVAILABLE_DATA_TYPES), resolveFeatureTypeIds(dataTypes));
  }

  private static void assertGeneDonor(JsonNode donor, FeatureType expectedFeatureType, int expectedValue) {
    assertGeneDonor(donor, Collections.singletonMap(expectedFeatureType, expectedValue));
  }

  public static void assertGeneDonor(JsonNode donor, Map<FeatureType, Integer> featureTypeCounts) {
    val summary = asObjectNode(donor).get(GENE_DONOR_SUMMARY);
    for (val featureType : FeatureType.values()) {
      val expectedValue = featureTypeCounts.get(featureType);
      if (expectedValue != null) {
        assertFeatrueTypeSummaryField(asObjectNode(summary), featureType, expectedValue);
      } else {
        assertFeatrueTypeSummaryField(asObjectNode(summary), featureType, 0);
      }
    }
  }

  private static void assertFeatrueTypeSummaryField(ObjectNode summary, FeatureType featureType, int expectedValue) {
    val field = summary.path(featureType.getSummaryFieldName());
    val expectedValueNode = createFeatureTypeSummaryValue(featureType, expectedValue);
    assertThat(field).isEqualTo(expectedValueNode);
  }

  private static ArrayNode getGeneDonors(ObjectNode gene) {
    val donors = gene.get(GENE_DONORS);
    assertThat(donors.isMissingNode()).isFalse();

    return asArrayNode(donors);
  }

  private static ArrayNode getGeneProjects(ObjectNode gene) {
    val donors = gene.get(GENE_PROJECTS);
    assertThat(donors.isMissingNode()).isFalse();

    return asArrayNode(donors);
  }

  private static void assertG3gene(ObjectNode gene) {
    val donors = getGeneDonors(gene);
    assertThat(donors).hasSize(2);
    for (val donor : donors) {
      assertGeneDonor(donor, SSM_TYPE, 1);
    }

    // Projects
    val projects = getGeneProjects(gene);
    assertThat(projects).hasSize(2);
    for (val project : projects) {
      val projectId = textValue(project, GENE_PROJECT_PROJECT_ID);
      assertThat(asList(BRCA_PROJECT_NAME, US_PROJECT_NAME)).contains(projectId);
      assertGeneProject(asObjectNode(project.get(GENE_PROJECT_SUMMARY)), 1, asList(SSM_TYPE));
    }
  }

  private static void assertG2gene(ObjectNode gene) {
    val donors = getGeneDonors(gene);
    assertThat(donors).hasSize(4);
    for (val donor : donors) {
      val donorId = textValue(donor, DONOR_ID);
      switch (donorId) {
      case "DO001":
        assertGeneDonor(donor, SSM_TYPE, 2);
        break;
      case "DO002":
        assertGeneDonor(donor, SGV_TYPE, 1);
        break;
      case "DO003":
        assertGeneDonor(donor, SSM_TYPE, 2);
        break;
      case "DO004":
        assertGeneDonor(donor, SGV_TYPE, 1);
        break;
      }
    }

    // Projects
    val projects = getGeneProjects(gene);
    assertThat(projects).hasSize(2);
    for (val project : projects) {
      val projectId = textValue(project, GENE_PROJECT_PROJECT_ID);
      assertThat(asList(BRCA_PROJECT_NAME, US_PROJECT_NAME)).contains(projectId);
      assertGeneProject(asObjectNode(project.get(GENE_PROJECT_SUMMARY)), 2, asList(SSM_TYPE, SGV_TYPE));
    }
  }

  private void assertProjects() {
    val projectSummary = produces(FileType.PROJECT_SUMMARY);
    assertThat(projectSummary).hasSize(2);
    log.debug("Projects Summary - {}", projectSummary);
    projectSummary.stream()
        .forEach(SummarizeJobTest::assertProjectSummary);

  }

  private static void assertProjectSummary(ObjectNode project) {
    val summary = project.path(PROJECT_SUMMARY);
    assertThat(summary.isMissingNode()).isFalse();
    val projectId = textValue(project, PROJECT_ID);
    if (projectId.equals(BRCA_PROJECT_NAME)) {
      assertBRCAProjectSummary(asObjectNode(summary));
    } else {
      assertUSProjectSummary(asObjectNode(summary));
    }
  }

  /**
   * @param asObjectNode
   */
  private static void assertUSProjectSummary(ObjectNode summary) {
    assertArray(summary.get(AVAILABLE_DATA_TYPES), emptyList());
    for (val featureType : FeatureType.values()) {
      assertFeatureTypeCount(featureType, summary, 0);
    }

    assertThat(summary.get(TOTAL_DONOR_COUNT).asInt()).isEqualTo(0);
    assertThat(summary.get(TOTAL_SAMPLE_COUNT).asInt()).isEqualTo(0);
    assertThat(summary.get(TOTAL_SPECIMEN_COUNT).asInt()).isEqualTo(0);
    assertThat(summary.get(TOTAL_LIVE_DONOR_COUNT).asInt()).isEqualTo(0);

    assertThat(textValue(summary, PROJECT_SUMMARY_STATE)).isEqualTo("pending");
    assertArray(summary.get(PROJECT_SUMMARY_REPOSITORY), emptyList());
    assertArray(summary.get(AVAILABLE_EXPERIMENTAL_ANALYSIS_PERFORMED), emptyList());
    assertThat(asObjectNode(summary.get(DONOR_SUMMARY_EXPERIMENTAL_ANALYSIS_SAMPLE_COUNTS))).isEmpty();
  }

  private static void assertBRCAProjectSummary(ObjectNode summary) {
    assertArray(summary.get(AVAILABLE_DATA_TYPES), resolveFeatureTypeIds(DO1_AVAILABLE_DATA_TYPES));
    for (val featureType : FeatureType.values()) {
      if (featureType.isSsm()) {
        assertFeatureTypeCount(featureType, summary, 2);
      } else if (DO1_AVAILABLE_DATA_TYPES.contains(featureType)) {
        assertFeatureTypeCount(featureType, summary, 1);
      } else {
        assertFeatureTypeCount(featureType, summary, 0);
      }
    }

    assertThat(summary.get(TOTAL_DONOR_COUNT).asInt()).isEqualTo(2);
    assertThat(summary.get(TOTAL_SAMPLE_COUNT).asInt()).isEqualTo(3);
    assertThat(summary.get(TOTAL_SPECIMEN_COUNT).asInt()).isEqualTo(2);
    assertThat(summary.get(TOTAL_LIVE_DONOR_COUNT).asInt()).isEqualTo(2);

    assertThat(textValue(summary, PROJECT_SUMMARY_STATE)).isEqualTo("live");
    assertArray(summary.get(PROJECT_SUMMARY_REPOSITORY), DO1_REPOS);
    assertArray(summary.get(AVAILABLE_EXPERIMENTAL_ANALYSIS_PERFORMED), DO1_EXPERIMENTAL_ANALYSIS_PERFORMED);
    assertExperimentalAnalysisCounts(asObjectNode(summary.get(DONOR_SUMMARY_EXPERIMENTAL_ANALYSIS_SAMPLE_COUNTS)));
  }

  private static void assertFeatureTypeCount(FeatureType featureType, ObjectNode summary, int expectedCount) {
    val field = summary.get(getTestedTypeCountFieldName(featureType));
    assertThat(field.asInt()).isEqualTo(expectedCount);
  }

  private void assertDonorSummary(String projectName) {
    val donorSummary = produces(projectName, FileType.DONOR_GENE_OBSERVATION_SUMMARY);
    log.debug("Donor Summary - {}", donorSummary);

    assertThat(donorSummary).hasSize(2);
    for (val donor : donorSummary) {
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

  private static void assertArray(JsonNode resultNode, Iterable<String> expectedValues) {
    val array = Streams.stream(asArrayNode(resultNode))
        .map(jn -> jn.textValue())
        .collect(Collectors.toList());

    assertThat(expectedValues).containsOnlyElementsOf(array);
  }

}
