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
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.List;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl2.core.job.FileType;
import org.icgc.dcc.etl2.test.job.AbstractJobTest;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

@Slf4j
public class JoinJobTest extends AbstractJobTest {

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

    // Clinical
    validateClinicalResults(produces(PROJECT_NAME, FileType.CLINICAL));
    validateEmptyProjectClinicalResults(produces(EMPTY_PROJECT_NAME, FileType.CLINICAL));

    // Observation
    validateObservation(produces(PROJECT_NAME, FileType.OBSERVATION));
  }

  private static void validateObservation(List<ObjectNode> results) {
    log.info("{}", results);
    assertThat(results).isNotEmpty();
  }

  private static void validateClinicalResults(List<ObjectNode> results) {
    assertThat(results).hasSize(1);
    val donor = results.get(0);
    validateTherapyFamilyExposure(donor.get("therapy"));
    validateTherapyFamilyExposure(donor.get("family"));
    validateTherapyFamilyExposure(donor.get("exposure"));
    validateSpecimen(donor.get("specimen"));
  }

  private static void validateSpecimen(JsonNode specimen) {
    assertThat(specimen.isArray()).isTrue();
    val specimens = copyOf(specimen.elements());
    assertThat(specimens).hasSize(1);

    val samples = copyOf(specimens.get(0).get("sample").elements());
    assertThat(samples).hasSize(2);

    samples.stream()
        .forEach(s -> verifySamples(s));
  }

  private static void validateTherapyFamilyExposure(JsonNode node) {
    assertThat(node.isArray()).isTrue();
    val elements = copyOf(node.elements());
    assertThat(elements).hasSize(1);
  }

  private static void verifySamples(JsonNode sample) {
    assertThat(VALID_SAMPLES).contains(sample.get("analyzed_sample_id").asText());
  }

  private static void validateEmptyProjectClinicalResults(List<ObjectNode> results) {
    assertThat(results).hasSize(1);
    val donor = results.get(0);
    assertThat(donor.findPath("therapy").isMissingNode()).isTrue();
    assertThat(donor.findPath("family").isMissingNode()).isTrue();
    assertThat(donor.findPath("exposure").isMissingNode()).isTrue();
    assertThat(donor.findPath("specimen").isMissingNode()).isTrue();
  }

}
