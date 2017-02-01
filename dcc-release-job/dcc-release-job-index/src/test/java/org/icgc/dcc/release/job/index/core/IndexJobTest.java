/*
 * Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.                             
 *                                                                                                               
 * This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
 * You should have received a copy of the GNU General Public License along with                                  
 * this program. If not, see <http://www.gnu.org/licenses/>.                                                     
 *                                                                                                               
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY                           
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES                          
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT                           
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,                                
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED                          
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;                               
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER                              
 * IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN                         
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.icgc.dcc.release.job.index.core;

import static com.google.common.base.Preconditions.checkState;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.dcc.common.es.TransportClientFactory.createClient;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import lombok.val;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.icgc.dcc.common.core.model.FieldNames;
import org.icgc.dcc.release.core.document.DocumentType;
import org.icgc.dcc.release.core.job.DefaultJobContext;
import org.icgc.dcc.release.core.job.Job;
import org.icgc.dcc.release.core.job.JobContext;
import org.icgc.dcc.release.core.job.JobType;
import org.icgc.dcc.release.job.index.config.IndexProperties;
import org.icgc.dcc.release.job.index.utils.IndexTasks;
import org.icgc.dcc.release.test.job.AbstractJobTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Table;

public class IndexJobTest extends AbstractJobTest {

  private static final String PROJECT = "BRCA-UK";
  private static final String ES_URI = "es://10.30.129.4:9300";

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
    val properties = new IndexProperties()
        .setEsUri(ES_URI)
        .setExportEsIndex(false);

    this.job = new IndexJob(properties);
    this.index = IndexTasks.getIndexName(RELEASE_VERSION);
    this.esClient = createClient(ES_URI, false);
    cleanUpIndex(esClient, index);
  }

  @After
  public void tearDown() {
    esClient.close();
  }

  @Test
  public void testExecute() {
    job.execute(createIndexJobContext(job.getType(), ImmutableList.of(PROJECT)));

    verifyRelease();
    verifyGeneSets();
    verifyProjects();
    verifyDonors();
    verifyGenes();
    verifyObservations();
    verifyMutations();
    verifyDrugs();
    verifyDrugSets();
  }

  private void verifyDrugSets() {
    verifyDrugSets(DocumentType.GENE_CENTRIC_TYPE, "drug", Optional.empty(), 1);
    verifyDrugSets(DocumentType.DONOR_CENTRIC_TYPE, "gene.drug", Optional.of("gene"), 1);
    verifyDrugSets(DocumentType.MUTATION_CENTRIC_TYPE, "transcript.gene.drug", Optional.of("transcript"), 2);
    verifyDrugSets(DocumentType.OBSERVATION_CENTRIC_TYPE, "ssm.gene.drug", Optional.of("ssm.gene"), 2);
  }

  private void verifyDrugSets(DocumentType type, String field, Optional<String> path, long expectedDocuments) {
    val existsQuery = QueryBuilders.existsQuery(field);
    val query = path.isPresent() ? QueryBuilders.nestedQuery(path.get(), existsQuery, ScoreMode.Avg) : existsQuery;

    val hits = esClient.prepareSearch(index)
        .setTypes(type.getName())
        .setQuery(query)
        .execute().actionGet()
        .getHits().getTotalHits();
    assertThat(hits).isEqualTo(expectedDocuments);
  }

  private void verifyDrugs() {
    verifyType(DocumentType.DRUG_CENTRIC_TYPE, 1);
    verifyType(DocumentType.DRUG_TEXT_TYPE, 1);
  }

  private void verifyType(DocumentType type, long expectedDocuments) {
    val hits = esClient.prepareSearch(index)
        .setTypes(type.getName()).execute().actionGet()
        .getHits().getTotalHits();
    assertThat(hits).isEqualTo(expectedDocuments);
  }

  private void verifyObservations() {
    verifyType(DocumentType.OBSERVATION_CENTRIC_TYPE, 3);
  }

  private void verifyProjects() {
    verifyType(DocumentType.PROJECT_TYPE, 2);
    verifyType(DocumentType.PROJECT_TEXT_TYPE, 2);
  }

  private void verifyGeneSets() {
    verifyType(DocumentType.GENE_SET_TYPE, 3);
    verifyType(DocumentType.GENE_SET_TEXT_TYPE, 3);
  }

  private void verifyGenes() {
    verifyType(DocumentType.GENE_TYPE, 3);
    verifyType(DocumentType.GENE_TEXT_TYPE, 3);
    verifyType(DocumentType.GENE_CENTRIC_TYPE, 3);
  }

  private void verifyMutations() {
    val hits = esClient.prepareSearch(index)
        // .setTypes(DocumentType.MUTATION_CENTRIC_TYPE.getName())
        .setTypes("mutation-centric").execute().actionGet();
    for (val hit : hits.getHits()) {
      val source = hit.getSource();
      verifyMutationCentric(source);
    }

    verifyType(DocumentType.MUTATION_TEXT_TYPE, 2);
  }

  private static void verifyMutationCentric(Map<String, Object> source) {
    assertThat(source.get(FieldNames.MUTATION_OCCURRENCES)).isNotNull();
    assertThat(source.get(FieldNames.MUTATION_TRANSCRIPTS)).isNotNull();
    assertThat(source.get(FieldNames.MUTATION_PLATFORM)).isNotNull();
    assertThat(source.get(FieldNames.MUTATION_SUMMARY)).isNotNull();
  }

  private void verifyRelease() {
    verifyType(DocumentType.RELEASE_TYPE, 1);
  }

  private void verifyDonors() {
    verifyDonor();
    verifyDonorCentric();
    verifyDonorText();
  }

  private void verifyDonorCentric() {
    verifyType(DocumentType.DONOR_CENTRIC_TYPE, 3);
  }

  private void verifyDonorText() {
    verifyType(DocumentType.DONOR_TEXT_TYPE, 3);
  }

  private void verifyDonor() {
    val donorsWithProjectCount = esClient
        .prepareSearch(index)
        // .setTypes(DocumentType.DONOR_TYPE.getName())
        .setTypes("donor")
        .setPostFilter(QueryBuilders.existsQuery("project"))
        .execute().actionGet().getHits().getTotalHits();
    assertThat(donorsWithProjectCount).isEqualTo(3L);
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

  @SuppressWarnings("unchecked")
  private JobContext createIndexJobContext(JobType type, List<String> projectNames) {
    return new DefaultJobContext(type, RELEASE_VERSION, projectNames, "/dev/null",
        new File(INPUT_TEST_FIXTURES_DIR).getAbsolutePath(), mock(Table.class), taskExecutor, true);
  }

}
