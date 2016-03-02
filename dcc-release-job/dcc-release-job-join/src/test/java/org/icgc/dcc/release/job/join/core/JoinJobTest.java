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
package org.icgc.dcc.release.job.join.core;

import static com.google.common.collect.ImmutableList.copyOf;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames.CONSEQUENCE_ARRAY_NAME;
import static org.icgc.dcc.release.test.util.TestJsonNodes.getElements;

import java.io.File;
import java.util.List;
import java.util.Optional;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.test.function.DonorJsonComparator;
import org.icgc.dcc.release.test.job.AbstractJobTest;
import org.icgc.dcc.release.test.util.SubmissionFiles;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Slf4j
public class JoinJobTest extends AbstractJobTest {

  private static final String PROJECT_NAME = "BRCA-UK";
  private static final String EMPTY_PROJECT_NAME = "EMPTY";

  JoinJob job;

  @Before
  @Override
  public void setUp() {
    super.setUp();
    this.job = new JoinJob(SubmissionFiles.getSchemas(), sparkContext);
  }

  @Test
  public void executeTest() {
    given(new File(INPUT_TEST_FIXTURES_DIR));
    val jobContext = createJobContext(job.getType(), asList(PROJECT_NAME, EMPTY_PROJECT_NAME));
    job.execute(jobContext);

    verifyResult(Optional.of(PROJECT_NAME), FileType.CLINICAL, new DonorJsonComparator());
    verifyResult(Optional.of(EMPTY_PROJECT_NAME), FileType.CLINICAL, new DonorJsonComparator());

    validateOccurrences();

    validatePrimaryMeta(produces(PROJECT_NAME, FileType.PEXP), 20);
    validatePrimaryMeta(produces(PROJECT_NAME, FileType.JCN), 45);
    validatePrimaryMeta(produces(PROJECT_NAME, FileType.MIRNA_SEQ), 30);
    validatePrimaryMeta(produces(PROJECT_NAME, FileType.METH_SEQ), 27);
    validatePrimaryMeta(produces(PROJECT_NAME, FileType.EXP_SEQ), 23);
    validatePrimaryMeta(produces(PROJECT_NAME, FileType.EXP_ARRAY), 18);
    validatePrimaryMeta(produces(PROJECT_NAME, FileType.METH_ARRAY), 21);

    verifyResult(PROJECT_NAME, FileType.CNSM);
    validateStsm(produces(PROJECT_NAME, FileType.STSM), 46);

    verifyResult(PROJECT_NAME, FileType.SSM);
    verifyResult(PROJECT_NAME, FileType.SGV);
  }

  private void validateOccurrences() {
    verifyResult(PROJECT_NAME, FileType.OBSERVATION);
  }

  private static void validateStsm(List<ObjectNode> produces, int fieldsCount) {
    validatePrimaryMeta(produces, fieldsCount);
    val occurrence = produces.get(0);
    assertThat(getElements(occurrence.get(CONSEQUENCE_ARRAY_NAME))).isEmpty();
  }

  private static void validatePrimaryMeta(List<ObjectNode> results, int expectedFieldsCount) {
    log.debug("{}", results);
    assertThat(results).hasSize(1);
    val joined = results.get(0);
    assertThat(keys(joined)).hasSize(expectedFieldsCount);
  }

  private static List<String> keys(JsonNode node) {
    if (node.isObject()) {
      val objectNode = (ObjectNode) node;
      return copyOf(objectNode.fieldNames());
    }

    return emptyList();
  }

}
