package org.icgc.dcc.release.job.index.core;

import static com.google.common.base.Preconditions.checkState;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.release.job.index.factory.TransportClientFactory.newTransportClient;

import java.io.File;
import java.util.Map;

import lombok.val;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilders;
import org.icgc.dcc.common.core.model.FieldNames;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.job.Job;
import org.icgc.dcc.release.job.index.config.IndexProperties;
import org.icgc.dcc.release.test.job.AbstractJobTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class IndexJobTest extends AbstractJobTest {

	private static final String PROJECT = "BRCA-UK";
	private static final String ES_URI = "es://localhost:9300";

	/**
	 * Class under test.
	 */
	Job job;
	Client esClient;
	String index;

	@Override
	@Before
	public void setUp() {
		super.setUp();
		val properties = new IndexProperties().setEsUri(ES_URI);

		this.job = new IndexJob(properties);
		this.index = IndexJob.resolveIndexName(RELEASE_VERSION);
		this.esClient = newTransportClient(ES_URI);
		cleanUpIndex(esClient, index);
	}



	@After
	public void tearDown() {
		esClient.close();
	}

	@Test
	public void testExecute() {
		given(new File(INPUT_TEST_FIXTURES_DIR));
		job.execute(createJobContext(job.getType(), ImmutableList.of(PROJECT)));

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
				// .setTypes(DocumentType.OBSERVATION_CENTRIC_TYPE.getName())
				.setTypes("observation-centric").execute().actionGet()
				.getHits().getTotalHits();
		assertThat(observations).isEqualTo(3L);
	}

	private void varifyProjects() {
		val projects = esClient.prepareSearch(index)
				// .setTypes(DocumentType.PROJECT_TYPE.getName())
				.setTypes("project").execute().actionGet().getHits()
				.getTotalHits();
		assertThat(projects).isEqualTo(2L);

		val projectsText = esClient.prepareSearch(index)
				// .setTypes(DocumentType.PROJECT_TEXT_TYPE.getName())
				.setTypes("project-text").execute().actionGet().getHits()
				.getTotalHits();
		assertThat(projectsText).isEqualTo(2L);
	}

	private void varifyGeneSets() {
		val geneSets = esClient.prepareSearch(index)
				// .setTypes(DocumentType.GENE_SET_TYPE.getName())
				.setTypes("gene-set").execute().actionGet().getHits()
				.getTotalHits();
		assertThat(geneSets).isEqualTo(3L);

		val geneSetsText = esClient.prepareSearch(index)
				// .setTypes(DocumentType.GENE_SET_TEXT_TYPE.getName())
				.setTypes("gene-set-text").execute().actionGet().getHits()
				.getTotalHits();
		assertThat(geneSetsText).isEqualTo(3L);
	}

	private void varifyGenes() {
		val genes = esClient.prepareSearch(index)
				// .setTypes(DocumentType.GENE_TYPE.getName())
				.setTypes("gene").execute().actionGet().getHits()
				.getTotalHits();
		assertThat(genes).isEqualTo(3L);

		val genesText = esClient.prepareSearch(index)
				// .setTypes(DocumentType.GENE_TEXT_TYPE.getName())
				.setTypes("gene-text").execute().actionGet().getHits()
				.getTotalHits();
		assertThat(genesText).isEqualTo(3L);

		val geneCentric = esClient.prepareSearch(index)
				// .setTypes(DocumentType.GENE_CENTRIC_TYPE.getName())
				.setTypes("gene-centric").execute().actionGet().getHits()
				.getTotalHits();
		assertThat(geneCentric).isEqualTo(3L);
	}

	private void verifyMutations() {
		verifyMutationsOutput();
		val hits = esClient.prepareSearch(index)
		// .setTypes(DocumentType.MUTATION_CENTRIC_TYPE.getName())
				.setTypes("mutation-centric").execute().actionGet();
		for (val hit : hits.getHits()) {
			val source = hit.getSource();
			verifyMutationCentric(source);
		}

		val mutationTextHits = esClient.prepareSearch(index)
				// .setTypes(DocumentType.MUTATION_TEXT_TYPE.getName())
				.setTypes("mutation-text").execute().actionGet().getHits()
				.getTotalHits();
		assertThat(mutationTextHits).isEqualTo(2L);
	}

	private void verifyMutationsOutput() {
		val mutationCentric = produces(FileType.MUTATION_CENTRIC_DOCUMENT);
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
				// .setTypes(DocumentType.RELEASE_TYPE.getName())
				.setTypes("release").execute().actionGet().getHits()
				.getTotalHits();
		assertThat(release).isEqualTo(1L);
	}

	private void verifyDonors() {
		verifyDonor();
		verifyDonorCentric();
		verifyDonorText();
	}

	private void verifyDonorCentric() {
		val donorsWithProjectCount = esClient.prepareSearch(index)
				// .setTypes(DocumentType.DONOR_CENTRIC_TYPE.getName())
				.setTypes("donor-centric").execute().actionGet().getHits()
				.getTotalHits();
		assertThat(donorsWithProjectCount).isEqualTo(2L);

		val donors = produces(PROJECT, FileType.DONOR_CENTRIC_DOCUMENT);
		val sgvDonor = donors.get(0);
		val sgvGeneDonorId = sgvDonor.get(FieldNames.DONOR_GENES).get(0)
				.get("sgv").get(0).path(FieldNames.DONOR_ID);
		assertThat(sgvGeneDonorId.isMissingNode()).isTrue();

		val ssmDonor = donors.get(1);
		val ssmGeneDonorId = ssmDonor.get(FieldNames.DONOR_GENES).get(0)
				.get("ssm").get(0).path(FieldNames.DONOR_ID);
		assertThat(ssmGeneDonorId.isMissingNode()).isTrue();
	}

	private void verifyDonorText() {
		val donorsCount = esClient.prepareSearch(index)
				// .setTypes(DocumentType.DONOR_TEXT_TYPE.getName())
				.setTypes("donor-text").execute().actionGet().getHits()
				.getTotalHits();
		assertThat(donorsCount).isEqualTo(2L);
	}

	private void verifyDonor() {
		val donorsWithProjectCount = esClient
				.prepareSearch(index)
				// .setTypes(DocumentType.DONOR_TYPE.getName())
				.setTypes("donor")
				.setPostFilter(FilterBuilders.existsFilter("project"))
				.execute().actionGet().getHits().getTotalHits();
		assertThat(donorsWithProjectCount).isEqualTo(2L);
	}
	
	private static void cleanUpIndex(Client esClient, String index) {
		val client = esClient.admin().indices();
	    boolean exists = client.prepareExists(index)
	        .execute()
	        .actionGet()
	        .isExists();

	    if (exists) {
	      checkState(client.prepareDelete(index)
	          .execute()
	          .actionGet()
	          .isAcknowledged(),
	          "Index '%s' deletion was not acknowledged", index);
	    }
	}

}
