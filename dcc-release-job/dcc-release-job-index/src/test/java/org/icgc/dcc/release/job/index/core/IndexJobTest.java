package org.icgc.dcc.release.job.index.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.release.job.index.core.IndexJob.resolveIndexName;
import static org.icgc.dcc.release.job.index.factory.TransportClientFactory.newTransportClient;
import static org.icgc.dcc.release.job.index.utils.TestUtils.createSnpEffProperties;

import java.io.File;
import java.util.Map;

import lombok.val;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilders;
import org.icgc.dcc.common.core.model.FieldNames;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.job.index.config.IndexProperties;
import org.icgc.dcc.release.job.index.model.DocumentType;
import org.icgc.dcc.release.test.job.AbstractJobTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class IndexJobTest extends AbstractJobTest {

  private static final String ES_URI = "es://localhost:9300";

  /**
   * Class under test.
   */
  IndexJob job;
  Client esClient;
  String index;

  @Override
  @Before
  public void setUp() {
    super.setUp();
    val properties = new IndexProperties()
        .setEsUri(ES_URI)
        .setOutputDir(new File(workingDir, "output").getAbsolutePath());

    this.job = new IndexJob(properties, createSnpEffProperties());
    this.index = resolveIndexName(RELEASE_VERSION);
    this.esClient = newTransportClient(ES_URI);
  }

  @After
  public void tearDown() {
    esClient.close();
  }

  @Test
  public void testExecute() {
    given(new File(TEST_FIXTURES_DIR));
    job.execute(createJobContext(job.getType(), ImmutableList.of("BRCA-UK")));

    verifyRelease();
    varifyGeneSets();
    varifyProjects();
    verifyDonors();
    varifyGenes();
    varifyObservations();
    verifyMutations();

  }

  private void varifyObservations() {
    val observations = esClient.prepareSearch(index)
        .setTypes(DocumentType.OBSERVATION_CENTRIC_TYPE.getName())
        .execute()
        .actionGet()
        .getHits()
        .getTotalHits();
    assertThat(observations).isEqualTo(3L);
  }

  private void varifyProjects() {
    val projects = esClient.prepareSearch(index)
        .setTypes(DocumentType.PROJECT_TYPE.getName())
        .execute()
        .actionGet()
        .getHits()
        .getTotalHits();
    assertThat(projects).isEqualTo(2L);

    val projectsText = esClient.prepareSearch(index)
        .setTypes(DocumentType.PROJECT_TEXT_TYPE.getName())
        .execute()
        .actionGet()
        .getHits()
        .getTotalHits();
    assertThat(projectsText).isEqualTo(2L);
  }

  private void varifyGeneSets() {
    val geneSets = esClient.prepareSearch(index)
        .setTypes(DocumentType.GENE_SET_TYPE.getName())
        .execute()
        .actionGet()
        .getHits()
        .getTotalHits();
    assertThat(geneSets).isEqualTo(3L);

    val geneSetsText = esClient.prepareSearch(index)
        .setTypes(DocumentType.GENE_SET_TEXT_TYPE.getName())
        .execute()
        .actionGet()
        .getHits()
        .getTotalHits();
    assertThat(geneSetsText).isEqualTo(3L);
  }

  private void varifyGenes() {
    val genes = esClient.prepareSearch(index)
        .setTypes(DocumentType.GENE_TYPE.getName())
        .execute()
        .actionGet()
        .getHits()
        .getTotalHits();
    assertThat(genes).isEqualTo(3L);

    val genesText = esClient.prepareSearch(index)
        .setTypes(DocumentType.GENE_TEXT_TYPE.getName())
        .execute()
        .actionGet()
        .getHits()
        .getTotalHits();
    assertThat(genesText).isEqualTo(3L);

    val geneCentric = esClient.prepareSearch(index)
        .setTypes(DocumentType.GENE_CENTRIC_TYPE.getName())
        .execute()
        .actionGet()
        .getHits()
        .getTotalHits();
    assertThat(geneCentric).isEqualTo(3L);
  }

  private void verifyMutations() {
    verifyMutationsOutput();
    val hits = esClient.prepareSearch(index)
        .setTypes(DocumentType.MUTATION_CENTRIC_TYPE.getName())
        .execute()
        .actionGet();
    for (val hit : hits.getHits()) {
      val source = hit.getSource();
      verifyMutationCentric(source);
    }

    val mutationTextHits = esClient.prepareSearch(index)
        .setTypes(DocumentType.MUTATION_TEXT_TYPE.getName())
        .execute()
        .actionGet()
        .getHits()
        .getTotalHits();
    assertThat(mutationTextHits).isEqualTo(2L);
  }

  private void verifyMutationsOutput() {
    val mutationCentric = produces(FileType.MUTATION_CENTRIC_INDEX);
    assertThat(mutationCentric).hasSize(2);
  }

  private static void verifyMutationCentric(Map<String, Object> source) {
    assertThat(source.get(FieldNames.MUTATION_OCCURRENCES)).isNotNull();
    assertThat(source.get(FieldNames.MUTATION_TRANSCRIPTS)).isNotNull();
    assertThat(source.get(FieldNames.MUTATION_PLATFORM)).isNotNull();
    assertThat(source.get(FieldNames.MUTATION_SUMMARY)).isNotNull();
  }

  private void verifyRelease() {
    val release = esClient.prepareSearch(index)
        .setTypes(DocumentType.RELEASE_TYPE.getName())
        .execute()
        .actionGet()
        .getHits()
        .getTotalHits();
    assertThat(release).isEqualTo(1L);
  }

  private void verifyDonors() {
    verifyDonor();
    verifyDonorCentric();
    verifyDonorText();
  }

  private void verifyDonorCentric() {
    val donorsWithProjectCount = esClient
        .prepareSearch(index)
        .setTypes(DocumentType.DONOR_CENTRIC_TYPE.getName())
        .execute()
        .actionGet()
        .getHits()
        .getTotalHits();
    assertThat(donorsWithProjectCount).isEqualTo(2L);
  }

  private void verifyDonorText() {
    val donorsCount = esClient
        .prepareSearch(index)
        .setTypes(DocumentType.DONOR_TEXT_TYPE.getName())
        .execute()
        .actionGet()
        .getHits()
        .getTotalHits();
    assertThat(donorsCount).isEqualTo(2L);
  }

  private void verifyDonor() {
    val donorsWithProjectCount = esClient
        .prepareSearch(index)
        .setTypes(DocumentType.DONOR_TYPE.getName())
        .setPostFilter(FilterBuilders.existsFilter("project"))
        .execute()
        .actionGet()
        .getHits()
        .getTotalHits();
    assertThat(donorsWithProjectCount).isEqualTo(2L);
  }

}
