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
package org.icgc.dcc.release.job.mask.function;

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.common.core.model.FieldNames.NormalizerFieldNames.NORMALIZER_MARKING;
import static org.icgc.dcc.common.core.model.FieldNames.NormalizerFieldNames.NORMALIZER_OBSERVATION_ID;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_CONTROL_GENOTYPE;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_MUTATED_FROM_ALLELE;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_REFERENCE_GENOME_ALLELE;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_TUMOUR_GENOTYPE;
import static org.icgc.dcc.common.core.model.Marking.CONTROLLED;

import java.util.UUID;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.icgc.dcc.common.core.model.Marking;
import org.icgc.dcc.common.core.model.SpecialValue;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;

/**
 * Steps in charge of creating a "masked" counterpart to sensitive rows (see {@link MarkSensitiveRow}).
 */
@Slf4j
public class GenerateMaskedRow implements FlatMapFunction<ObjectNode, ObjectNode> {

  @Override
  public Iterable<ObjectNode> call(ObjectNode row) throws Exception {
    // Create masked counterpart if sensitive (see
    // https://wiki.oicr.on.ca/display/DCCSOFT/Data+Normalizer+Component?focusedCommentId=53182773#comment-53182773)

    val rows = Lists.<ObjectNode> newArrayList();
    rows.add(row);

    if (getMarkingState(row) == CONTROLLED) {
      val referenceGenomeAllele = row.get(SUBMISSION_OBSERVATION_REFERENCE_GENOME_ALLELE).textValue();

      log.debug("Creating mask for '{}'", row);
      val mask = mask(row.deepCopy(), referenceGenomeAllele);
      regenerateId(mask);

      log.debug("Resulting mask for '{}': '{}'", row, mask);
      rows.add(mask);
    }

    return rows;
  }

  /**
   * Creates a row corresponding to a masked version of the observation.
   */
  private ObjectNode mask(ObjectNode copy, String referenceGenomeAllele) {
    // Empty the two genotype fields
    copy.put(SUBMISSION_OBSERVATION_CONTROL_GENOTYPE, (String) SpecialValue.NO_VALUE);
    copy.put(SUBMISSION_OBSERVATION_TUMOUR_GENOTYPE, (String) SpecialValue.NO_VALUE);

    copy.put(SUBMISSION_OBSERVATION_MUTATED_FROM_ALLELE, referenceGenomeAllele);
    copy.put(NORMALIZER_MARKING, Marking.MASKED.getTupleValue());

    return copy;
  }

  /**
   * Returns the value for masking as set by the previous step.
   */
  private Marking getMarkingState(ObjectNode entry) {
    val markingString = entry.get(NORMALIZER_MARKING).textValue();
    val marking = Marking.from(markingString);
    checkState(marking.isPresent(), "There should be a '%s' field at this stage, instead: '%s'", NORMALIZER_MARKING,
        entry);
    return marking.get();
  }

  private static void regenerateId(ObjectNode mask) {
    mask.put(NORMALIZER_OBSERVATION_ID, UUID.randomUUID().toString());
  }

}
