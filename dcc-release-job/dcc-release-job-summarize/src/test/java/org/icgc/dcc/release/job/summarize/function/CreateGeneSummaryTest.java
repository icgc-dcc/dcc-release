package org.icgc.dcc.release.job.summarize.function;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.core.model.FeatureTypes.FeatureType.SGV_TYPE;
import static org.icgc.dcc.common.core.model.FeatureTypes.FeatureType.SSM_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.AFFECTED_DONOR_COUNT;
import static org.icgc.dcc.common.core.model.FieldNames.AVAILABLE_DATA_TYPES;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_DONORS;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_DONOR_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_PROJECTS;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_PROJECT_SUMMARY;
import static org.icgc.dcc.common.json.Jackson.asArrayNode;
import static org.icgc.dcc.common.json.Jackson.asObjectNode;
import static org.icgc.dcc.release.core.util.FeatureTypes.createFeatureTypeSummaryValue;
import static org.icgc.dcc.release.test.util.TestJsonNodes.$;
import static org.icgc.dcc.release.test.util.TestJsonNodes.getElements;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.core.model.FeatureTypes.FeatureType;
import org.icgc.dcc.common.core.util.stream.Streams;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;

@Slf4j
public class CreateGeneSummaryTest {

  CreateGeneSummary function = new CreateGeneSummary();

  @Test
  public void callTest() throws Exception {
    val geneStats =
        $("{donor:[{_donor_id:'DO001',sgv:1,_project_id:'BRCA-UK'},{_donor_id:'DO001',ssm:2,_project_id:'BRCA-UK'},{_donor_id:'DO003',ssm:2,_project_id:'ALL-US'},{_donor_id:'DO004',sgv:1,_project_id:'ALL-US'}]}");
    val result = function.call(geneStats);
    log.info("Result - {}", result);
    assertThat(result).hasSize(2);

    val donors = getElements(result.get(GENE_DONORS));
    assertThat(donors).hasSize(3);
    assertGeneDonor(asObjectNode(donors.get(0)), ImmutableMap.of(SSM_TYPE, 2, SGV_TYPE, 1));
    assertGeneDonor(asObjectNode(donors.get(1)), ImmutableMap.of(SSM_TYPE, 2));
    assertGeneDonor(asObjectNode(donors.get(2)), ImmutableMap.of(SGV_TYPE, 1));

    val projects = getElements(result.get(GENE_PROJECTS));
    assertThat(projects).hasSize(2);
    assertGeneProject(asObjectNode(projects.get(0).get(GENE_PROJECT_SUMMARY)), 1, Arrays.asList(SSM_TYPE, SGV_TYPE));
    assertGeneProject(asObjectNode(projects.get(1).get(GENE_PROJECT_SUMMARY)), 2, Arrays.asList(SSM_TYPE, SGV_TYPE));
  }

  private static void assertGeneProject(ObjectNode projectSummary, int expectedDonorCount, List<FeatureType> dataTypes) {
    val donorCount = projectSummary.get(AFFECTED_DONOR_COUNT).asInt();
    assertThat(donorCount).isEqualTo(expectedDonorCount);
    assertArray(projectSummary.get(AVAILABLE_DATA_TYPES), resolveFeatureTypeIds(dataTypes));
  }

  private static List<String> resolveFeatureTypeIds(List<FeatureType> featureTypes) {
    return featureTypes.stream()
        .map(ft -> ft.getId())
        .collect(toList());
  }

  private static void assertArray(JsonNode resultNode, Iterable<String> expectedValues) {
    val array = Streams.stream(asArrayNode(resultNode))
        .map(jn -> jn.textValue())
        .collect(Collectors.toList());

    assertThat(expectedValues).containsOnlyElementsOf(array);
  }

  private static void assertGeneDonor(JsonNode donor, Map<FeatureType, Integer> featureTypeCounts) {
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

}
