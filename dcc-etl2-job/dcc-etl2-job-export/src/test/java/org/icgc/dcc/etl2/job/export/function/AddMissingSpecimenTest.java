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
package org.icgc.dcc.etl2.job.export.function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.etl2.core.util.ObjectNodes.toEmptyJsonValue;
import static org.icgc.dcc.etl2.job.export.model.type.Constants.SPECIMEN_FIELD_NAME;
import static org.icgc.dcc.etl2.test.util.TestJsonNodes.$;
import lombok.val;

import org.icgc.dcc.etl2.core.function.AddMissingField;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class AddMissingSpecimenTest {

  private static final ImmutableMap<String, String> SECOND_LEVEL_PROJECTION = ImmutableMap.<String, String> builder()
      // .put("donor_id", "donor_id")
      .put("gene_affected", "gene_affected")
      .put("transcript_affected", "transcript_affected")
      .put("gene_build_version", "gene_build_version")
      .build();

  private static final String EMPTY_SPECIMEN_VALUE = toEmptyJsonValue(SECOND_LEVEL_PROJECTION.keySet());

  @Test
  public void testAddDonorIdField() throws Exception {
    val addMissingSpecimen = new AddMissingField(SPECIMEN_FIELD_NAME, SECOND_LEVEL_PROJECTION.keySet());
    val input = $("{icgc_donor_id: 'DO5', y: 2}");
    val actual = addMissingSpecimen.call(input);
    val expectedJson = String.format("{icgc_donor_id: 'DO5', y: 2, specimen: '%s'}", EMPTY_SPECIMEN_VALUE);
    val expected = $(expectedJson);

    assertThat(actual)
        .isNotSameAs(expected)
        .isEqualTo(expected);
  }

}
