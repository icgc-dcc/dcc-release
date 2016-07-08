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
package org.icgc.dcc.release.job.mask.core;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.core.model.FieldNames.NormalizerFieldNames.NORMALIZER_OBSERVATION_ID;
import static org.icgc.dcc.release.test.util.TestJsonNodes.$;

import java.io.File;
import java.util.UUID;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.job.Job;
import org.icgc.dcc.release.test.job.AbstractJobTest;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;

@Slf4j
public class MaskJobTest extends AbstractJobTest {

  private static final String PROJECT_NAME = "BRCA-UK";

  Job job;

  @Before
  @Override
  public void setUp() {
    super.setUp();
    this.job = new MaskJob();
  }

  @Test
  public void executeTest() {
    given(new File(TEST_FIXTURES_DIR));
    val jobContext = createJobContext(job.getType(), asList(PROJECT_NAME));
    job.execute(jobContext);

    val result = produces(PROJECT_NAME, FileType.SSM_P_MASKED);
    log.debug("Results: {}", result);
    val controlledMutation = result.get(0);
    val controlledObservationId = controlledMutation.get(NORMALIZER_OBSERVATION_ID).textValue();
    val expectedControlled =
        $("{reference_genome_allele:'G',control_genotype:'G/T',mutated_from_allele:'T',mutated_to_allele:"
            + "'A',tumour_genotype:'G/A',marking:'CONTROLLED',mutation:'T>A'}");
    assertControlled(controlledMutation, expectedControlled);

    val maskedMutation = result.get(1);
    val maskedObservationId = maskedMutation.get(NORMALIZER_OBSERVATION_ID).textValue();
    assertThat(maskedObservationId).isNotEqualTo(controlledObservationId);

    val expectedMasked =
        $("{reference_genome_allele:'G',control_genotype:null,mutated_from_allele:'G',mutated_to_allele:"
            + "'A',tumour_genotype:null,marking:'MASKED',mutation:'G>A'}");
    assertControlled(maskedMutation, expectedMasked);
  }

  private static void assertControlled(ObjectNode mutation, ObjectNode expected) {
    val controlledObservationId = mutation.remove(NORMALIZER_OBSERVATION_ID).textValue();
    // Will throw exception if it's not UUID
    UUID.fromString(controlledObservationId);
    assertThat(mutation).isEqualTo(expected);
  }

}
