/*
 * Copyright (c) 2015 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.etl2.job.join.core;

import static com.google.common.collect.ImmutableList.copyOf;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SAMPLE;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SPECIMEN;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_VERIFICATION_STATUS;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_CONSEQUENCES_CONSEQUENCE_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_PLATFORM;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_SEQUENCING_STRATEGY;
import static org.icgc.dcc.common.core.model.FieldNames.OBSERVATION_VERIFICATION_PLATFORM;
import static org.icgc.dcc.common.core.model.FieldNames.AnnotatorFieldNames.ANNOTATOR_AMINO_ACID_CHANGE;
import static org.icgc.dcc.common.core.model.FieldNames.AnnotatorFieldNames.ANNOTATOR_CDS_CHANGE;
import static org.icgc.dcc.common.core.model.FieldNames.AnnotatorFieldNames.ANNOTATOR_GENE_BUILD_VERSION;
import static org.icgc.dcc.common.core.model.FieldNames.AnnotatorFieldNames.ANNOTATOR_NOTE;
import static org.icgc.dcc.common.core.model.FieldNames.AnnotatorFieldNames.ANNOTATOR_PROTEIN_DOMAIN_AFFECTED;
import static org.icgc.dcc.common.core.model.FieldNames.IdentifierFieldNames.SURROGATE_DONOR_ID;
import static org.icgc.dcc.common.core.model.FieldNames.IdentifierFieldNames.SURROGATE_MUTATION_ID;
import static org.icgc.dcc.common.core.model.FieldNames.IdentifierFieldNames.SURROGATE_SAMPLE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.IdentifierFieldNames.SURROGATE_SPECIMEN_ID;
import static org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames.AVAILABLE_RAW_SEQUENCE_DATA;
import static org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames.CONSEQUENCE_ARRAY_NAME;
import static org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames.GENE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames.OBSERVATION_ARRAY_NAME;
import static org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames.OBSERVATION_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames.PROJECT_ID;
import static org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames.SURROGATE_MATCHED_SAMPLE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames.TRANSCRIPT_ID;
import static org.icgc.dcc.common.core.model.FieldNames.NormalizerFieldNames.NORMALIZER_MARKING;
import static org.icgc.dcc.common.core.model.FieldNames.NormalizerFieldNames.NORMALIZER_MUTATION;
import static org.icgc.dcc.common.core.model.FieldNames.NormalizerFieldNames.NORMALIZER_OBSERVATION_ID;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_ANALYZED_SAMPLE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_GENE_AFFECTED;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_MATCHED_SAMPLE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_ANALYSIS_ID;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_ASSEMBLY_VERSION;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_CHROMOSOME;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_CHROMOSOME_END;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_CHROMOSOME_START;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_CHROMOSOME_STRAND;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_CONTROL_GENOTYPE;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_MUTATED_FROM_ALLELE;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_MUTATED_TO_ALLELE;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_MUTATION_TYPE;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_RAW_DATA_ACCESSION;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_RAW_DATA_REPOSITORY;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_REFERENCE_GENOME_ALLELE;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_TUMOUR_GENOTYPE;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_TRANSCRIPT_AFFECTED;
import static org.icgc.dcc.etl2.core.util.FieldNames.JoinFieldNames.ALIGNMENT_ALGORITHM;
import static org.icgc.dcc.etl2.core.util.FieldNames.JoinFieldNames.BASE_CALLING_ALGORITHM;
import static org.icgc.dcc.etl2.core.util.FieldNames.JoinFieldNames.BIOLOGICAL_VALIDATION_PLATFORM;
import static org.icgc.dcc.etl2.core.util.FieldNames.JoinFieldNames.BIOLOGICAL_VALIDATION_STATUS;
import static org.icgc.dcc.etl2.core.util.FieldNames.JoinFieldNames.EXPERIMENTAL_PROTOCOL;
import static org.icgc.dcc.etl2.core.util.FieldNames.JoinFieldNames.EXPOSURE;
import static org.icgc.dcc.etl2.core.util.FieldNames.JoinFieldNames.EXPRESSED_ALLELE;
import static org.icgc.dcc.etl2.core.util.FieldNames.JoinFieldNames.FAMILY;
import static org.icgc.dcc.etl2.core.util.FieldNames.JoinFieldNames.MUTANT_ALLELE_READ_COUNT;
import static org.icgc.dcc.etl2.core.util.FieldNames.JoinFieldNames.OTHER_ANALYSIS_ALGORITHM;
import static org.icgc.dcc.etl2.core.util.FieldNames.JoinFieldNames.PROBABILITY;
import static org.icgc.dcc.etl2.core.util.FieldNames.JoinFieldNames.QUALITY_SCORE;
import static org.icgc.dcc.etl2.core.util.FieldNames.JoinFieldNames.SEQ_COVERAGE;
import static org.icgc.dcc.etl2.core.util.FieldNames.JoinFieldNames.THERAPY;
import static org.icgc.dcc.etl2.core.util.FieldNames.JoinFieldNames.TOTAL_READ_COUNT;
import static org.icgc.dcc.etl2.core.util.FieldNames.JoinFieldNames.VARIATION_CALLING_ALGORITHM;
import static org.icgc.dcc.etl2.core.util.ObjectNodes.textValue;
import static org.icgc.dcc.etl2.test.util.TestJsonNodes.getElements;

import java.io.File;
import java.util.List;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl2.core.job.FileType;
import org.icgc.dcc.etl2.core.util.ObjectNodes;
import org.icgc.dcc.etl2.test.job.AbstractJobTest;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

@Slf4j
public class JoinJobTest extends AbstractJobTest {

  private static final ImmutableList<String> VALID_CONSEQUENCE_FIELDS = ImmutableList.of(
      GENE_ID,
      TRANSCRIPT_ID,
      ANNOTATOR_AMINO_ACID_CHANGE,
      ANNOTATOR_CDS_CHANGE,
      OBSERVATION_CONSEQUENCES_CONSEQUENCE_TYPE,
      SUBMISSION_GENE_AFFECTED,
      ANNOTATOR_GENE_BUILD_VERSION,
      ANNOTATOR_NOTE,
      ANNOTATOR_PROTEIN_DOMAIN_AFFECTED,
      SUBMISSION_TRANSCRIPT_AFFECTED);

  private static final ImmutableList<String> VALID_OBSERVATION_FIELDS = ImmutableList.of(
      BIOLOGICAL_VALIDATION_PLATFORM,
      EXPRESSED_ALLELE,
      PROBABILITY,
      QUALITY_SCORE,
      EXPERIMENTAL_PROTOCOL,
      OBSERVATION_VERIFICATION_PLATFORM,
      SUBMISSION_OBSERVATION_CONTROL_GENOTYPE,
      SUBMISSION_OBSERVATION_TUMOUR_GENOTYPE,
      SURROGATE_MATCHED_SAMPLE_ID,
      SURROGATE_SAMPLE_ID,
      SURROGATE_SPECIMEN_ID,
      ALIGNMENT_ALGORITHM,
      SUBMISSION_OBSERVATION_ANALYSIS_ID,
      SUBMISSION_ANALYZED_SAMPLE_ID,
      BASE_CALLING_ALGORITHM,
      BIOLOGICAL_VALIDATION_STATUS,
      NORMALIZER_MARKING,
      SUBMISSION_MATCHED_SAMPLE_ID,
      MUTANT_ALLELE_READ_COUNT,
      NORMALIZER_OBSERVATION_ID,
      OTHER_ANALYSIS_ALGORITHM,
      OBSERVATION_PLATFORM,
      SUBMISSION_OBSERVATION_RAW_DATA_ACCESSION,
      SUBMISSION_OBSERVATION_RAW_DATA_REPOSITORY,
      SEQ_COVERAGE,
      OBSERVATION_SEQUENCING_STRATEGY,
      TOTAL_READ_COUNT,
      VARIATION_CALLING_ALGORITHM,
      MUTATION_VERIFICATION_STATUS);

  private static final ImmutableList<String> VALID_OCCURRENCE_FIELDS = ImmutableList.of(
      SURROGATE_DONOR_ID,
      SURROGATE_MUTATION_ID,
      PROJECT_ID,
      OBSERVATION_TYPE,
      SUBMISSION_OBSERVATION_ASSEMBLY_VERSION,
      SUBMISSION_OBSERVATION_CHROMOSOME,
      SUBMISSION_OBSERVATION_CHROMOSOME_END,
      SUBMISSION_OBSERVATION_CHROMOSOME_START,
      SUBMISSION_OBSERVATION_CHROMOSOME_STRAND,
      CONSEQUENCE_ARRAY_NAME,
      SUBMISSION_OBSERVATION_MUTATED_FROM_ALLELE,
      SUBMISSION_OBSERVATION_MUTATED_TO_ALLELE,
      NORMALIZER_MUTATION,
      SUBMISSION_OBSERVATION_MUTATION_TYPE,
      OBSERVATION_ARRAY_NAME,
      SUBMISSION_OBSERVATION_REFERENCE_GENOME_ALLELE);

  private static final String PROJECT_NAME = "BRCA-UK";
  private static final String EMPTY_PROJECT_NAME = "EMPTY";
  private static final List<String> VALID_SAMPLES = ImmutableList.of("ASID1", "ASID2");

  JoinJob job;

  @Before
  @Override
  public void setUp() {
    super.setUp();
    this.job = new JoinJob();
  }

  @Test
  public void executeTest() {
    given(new File(TEST_FIXTURES_DIR));
    val jobContext = createJobContext(job.getType(), asList(PROJECT_NAME, EMPTY_PROJECT_NAME));
    job.execute(jobContext);

    validateClinicalResults(produces(PROJECT_NAME, FileType.CLINICAL));
    validateEmptyProjectClinicalResults(produces(EMPTY_PROJECT_NAME, FileType.CLINICAL));

    validateOccurrences(produces(PROJECT_NAME, FileType.OBSERVATION));

    validatePrimaryMeta(produces(PROJECT_NAME, FileType.PEXP), 20);
    validatePrimaryMeta(produces(PROJECT_NAME, FileType.JCN), 45);
    validatePrimaryMeta(produces(PROJECT_NAME, FileType.MIRNA_SEQ), 30);
    validatePrimaryMeta(produces(PROJECT_NAME, FileType.METH_SEQ), 27);
    validatePrimaryMeta(produces(PROJECT_NAME, FileType.EXP_SEQ), 23);
    validatePrimaryMeta(produces(PROJECT_NAME, FileType.EXP_ARRAY), 18);
    validatePrimaryMeta(produces(PROJECT_NAME, FileType.METH_ARRAY), 21);

    validateSecondaryType(produces(PROJECT_NAME, FileType.CNSM), 40);
    validateSecondaryType(produces(PROJECT_NAME, FileType.SGV), 34);
    validateStsm(produces(PROJECT_NAME, FileType.STSM), 46);

    validateSsm(produces(PROJECT_NAME, FileType.SSM));
  }

  private static void validateSsm(List<ObjectNode> results) {
    assertThat(results).hasSize(2);
    for (val occurrence : results) {
      assertThat(keys(occurrence)).hasSize(16);
      assertThat(occurrence.get(OBSERVATION_ARRAY_NAME)).isNotEmpty();
      assertThat(occurrence.get(CONSEQUENCE_ARRAY_NAME)).isNotEmpty();
    }

  }

  private static void validateStsm(List<ObjectNode> produces, int fieldsCount) {
    validatePrimaryMeta(produces, fieldsCount);
    val occurrence = produces.get(0);
    assertThat(getElements(occurrence.get(CONSEQUENCE_ARRAY_NAME))).isEmpty();
  }

  private static void validateSecondaryType(List<ObjectNode> results, int fieldsCount) {
    log.debug("{}", results);
    validatePrimaryMeta(results, fieldsCount);
    val occurrence = results.get(0);

    val consequences = occurrence.get(CONSEQUENCE_ARRAY_NAME);
    assertThat(consequences).isNotEmpty();
    assertThat(keys(consequences.get(0)).size()).isGreaterThan(2);
  }

  private static void validatePrimaryMeta(List<ObjectNode> results, int expectedFieldsCount) {
    log.debug("{}", results);
    assertThat(results).hasSize(1);
    val joined = results.get(0);
    assertThat(keys(joined)).hasSize(expectedFieldsCount);
  }

  private static void validateOccurrences(List<ObjectNode> results) {
    assertThat(results).hasSize(2);

    for (val occurrence : results) {
      validateOccurrenceStructure(occurrence);

      if (textValue(occurrence, SURROGATE_DONOR_ID).equals("DO001")) {
        validateDO1Occurrence(occurrence);
      }
      else {
        validateDO2Occurrence(occurrence);
      }
    }
  }

  private static void validateDO2Occurrence(ObjectNode occurrence) {
    assertThat(occurrence.get(OBSERVATION_ARRAY_NAME).size()).isEqualTo(2);
  }

  private static void validateDO1Occurrence(ObjectNode occurrence) {
    assertThat(occurrence.get(OBSERVATION_ARRAY_NAME).size()).isEqualTo(1);
  }

  private static void validateOccurrenceStructure(ObjectNode occurrence) {
    val fields = ImmutableList.copyOf(occurrence.fieldNames());
    assertThat(VALID_OCCURRENCE_FIELDS).containsOnlyElementsOf(fields);

    validateArrayNode(occurrence.get(CONSEQUENCE_ARRAY_NAME));
    validateArrayNode(occurrence.get(OBSERVATION_ARRAY_NAME));

    validateConsequenceStructure(occurrence.get(CONSEQUENCE_ARRAY_NAME).get(0));
    validateObservationStructure(occurrence.get(OBSERVATION_ARRAY_NAME).get(0));
  }

  private static void validateObservationStructure(JsonNode observation) {
    assertThat(VALID_OBSERVATION_FIELDS).containsOnlyElementsOf(getFields(observation));
  }

  private static void validateConsequenceStructure(JsonNode consequence) {
    assertThat(VALID_CONSEQUENCE_FIELDS).containsOnlyElementsOf(getFields(consequence));
  }

  private static List<String> getFields(JsonNode node) {
    return ImmutableList.copyOf(node.fieldNames());
  }

  private static void validateArrayNode(JsonNode arrayNode) {
    assertThat(arrayNode.isArray()).isTrue();
    assertThat(arrayNode).isNotEmpty();
  }

  private static void validateClinicalResults(List<ObjectNode> results) {
    log.debug("Clinical - {}", results);
    assertThat(results).hasSize(2);
    val donor = results.get(0);
    validateTherapyFamilyExposure(donor.get(THERAPY));
    validateTherapyFamilyExposure(donor.get(FAMILY));
    validateTherapyFamilyExposure(donor.get(EXPOSURE));
    validateSpecimen(donor.get(DONOR_SPECIMEN));
  }

  private static void validateSpecimen(JsonNode specimenArray) {
    assertThat(specimenArray.isArray()).isTrue();
    val specimens = getElements(specimenArray);
    assertThat(specimens).hasSize(1);

    val specimen = specimens.get(0);
    assertThat(keys(specimen)).hasSize(30);
    val samples = getElements(specimen.get(DONOR_SAMPLE));
    assertThat(samples).hasSize(2);

    samples.stream().forEach(s -> validateSamples(s));
  }

  private static void validateTherapyFamilyExposure(JsonNode node) {
    assertThat(node.isArray()).isTrue();
    val elements = getElements(node);
    assertThat(elements).hasSize(1);
  }

  private static void validateSamples(JsonNode sample) {
    val sampleId = ObjectNodes.textValue(sample, SUBMISSION_ANALYZED_SAMPLE_ID);
    assertThat(VALID_SAMPLES).contains(sampleId);
    // Sample has 9 fields
    assertThat(keys(sample)).hasSize(9);

    if (sampleId.equals("ASID1")) {
      verifyRawSequenceData(sample, 11);
    } else {
      verifyRawSequenceData(sample, 0);
    }

  }

  private static void verifyRawSequenceData(JsonNode sample, int expectedExementsCount) {
    val elements = getElements(sample.get(AVAILABLE_RAW_SEQUENCE_DATA));
    assertThat(elements).hasSize(expectedExementsCount);

    if (expectedExementsCount > 0) {
      elements.stream()
          .forEach(node -> {
            // Each RawSequenceData object has 3 fields
              assertThat(keys(node)).hasSize(3);
              assertThat(node.path(SUBMISSION_ANALYZED_SAMPLE_ID).isMissingNode()).isTrue();
            });
    }
  }

  private static void validateEmptyProjectClinicalResults(List<ObjectNode> results) {
    assertThat(results).hasSize(1);
    val donor = results.get(0);
    assertThat(donor.findPath(THERAPY).isMissingNode()).isTrue();
    assertThat(donor.findPath(FAMILY).isMissingNode()).isTrue();
    assertThat(donor.findPath(EXPOSURE).isMissingNode()).isTrue();
    assertThat(donor.findPath(DONOR_SPECIMEN).isMissingNode()).isTrue();
  }

  private static List<String> keys(JsonNode node) {
    if (node.isObject()) {
      val objectNode = (ObjectNode) node;
      return copyOf(objectNode.fieldNames());
    }

    return emptyList();
  }

}
