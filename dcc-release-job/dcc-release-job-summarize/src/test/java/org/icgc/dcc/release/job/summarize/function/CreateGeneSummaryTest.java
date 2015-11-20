package org.icgc.dcc.release.job.summarize.function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.core.model.FeatureTypes.FeatureType.SGV_TYPE;
import static org.icgc.dcc.common.core.model.FeatureTypes.FeatureType.SSM_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_DONORS;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_PROJECTS;
import static org.icgc.dcc.common.core.model.FieldNames.GENE_PROJECT_SUMMARY;
import static org.icgc.dcc.common.json.Jackson.asObjectNode;
import static org.icgc.dcc.release.job.summarize.core.SummarizeJobTest.assertGeneDonor;
import static org.icgc.dcc.release.job.summarize.core.SummarizeJobTest.assertGeneProject;
import static org.icgc.dcc.release.test.util.TestJsonNodes.$;
import static org.icgc.dcc.release.test.util.TestJsonNodes.getElements;

import java.util.Arrays;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.release.job.summarize.function.CreateGeneSummary;
import org.junit.Test;

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
}
