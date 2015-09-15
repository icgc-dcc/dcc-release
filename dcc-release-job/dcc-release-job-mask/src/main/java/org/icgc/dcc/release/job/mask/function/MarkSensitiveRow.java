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
import static com.google.common.collect.Sets.newLinkedHashSet;
import static org.icgc.dcc.common.core.model.FieldNames.NormalizerFieldNames.NORMALIZER_MARKING;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_CONTROL_GENOTYPE;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_MUTATED_TO_ALLELE;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_REFERENCE_GENOME_ALLELE;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_TUMOUR_GENOTYPE;
import static org.icgc.dcc.common.core.model.Marking.CONTROLLED;
import static org.icgc.dcc.common.core.model.Marking.OPEN;

import java.util.Set;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.function.Function;
import org.icgc.dcc.common.core.model.Marking;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Splitter;

@Slf4j
public class MarkSensitiveRow implements Function<ObjectNode, ObjectNode> {

  private static final Splitter ALLELES_SPLITTER = Splitter.on("/");

  @Override
  public ObjectNode call(ObjectNode row) throws Exception {
    val referenceGenomeAllele = row.get(SUBMISSION_OBSERVATION_REFERENCE_GENOME_ALLELE).textValue();
    val controlGenotype = row.get(SUBMISSION_OBSERVATION_CONTROL_GENOTYPE).textValue();
    val tumourGenotype = row.get(SUBMISSION_OBSERVATION_TUMOUR_GENOTYPE).textValue();
    val mutatedToAllele = row.get(SUBMISSION_OBSERVATION_MUTATED_TO_ALLELE).textValue();

    // Mark if applicable
    final Marking masking;
    if (!matchesAllControlAlleles(referenceGenomeAllele, controlGenotype)
        || !matchesAllTumourAllelesButTo(referenceGenomeAllele, tumourGenotype, mutatedToAllele)) {

      log.debug("Marking sensitive row: '{}'", row); // Should be rare enough
      masking = CONTROLLED;

    } else {
      log.debug("Marking open-access row: '{}'", row);
      masking = OPEN;
    }

    row.put(NORMALIZER_MARKING, masking.getTupleValue());

    return row;
  }

  private boolean matchesAllControlAlleles(String referenceGenomeAllele, String controlGenotype) {
    val controlAlleles = getUniqueAlleles(controlGenotype);
    for (val controlAllele : controlAlleles) {
      if (!referenceGenomeAllele.equals(controlAllele)) {
        return false;
      }
    }

    return true;
  }

  private boolean matchesAllTumourAllelesButTo(String referenceGenomeAllele, String tumourGenotype,
      String mutatedToAllele) {
    for (val tumourAllele : getTumourAllelesMinusToAllele(tumourGenotype, mutatedToAllele)) {
      if (!referenceGenomeAllele.equals(tumourAllele)) {
        return false;
      }
    }
    return true;
  }

  private Set<String> getTumourAllelesMinusToAllele(String tumourGenotype, String mutatedToAllele) {
    val alleles = getUniqueAlleles(tumourGenotype);
    val removed = alleles.remove(mutatedToAllele);
    checkState(
        removed,
        "'%s' ('%s') is expected to be in '%s' ('%s') as per primary validation rules",
        mutatedToAllele, SUBMISSION_OBSERVATION_MUTATED_TO_ALLELE, tumourGenotype,
        SUBMISSION_OBSERVATION_TUMOUR_GENOTYPE);
    return alleles;
  }

  private static Set<String> getUniqueAlleles(String controlGenotype) {
    return newLinkedHashSet(ALLELES_SPLITTER.split(controlGenotype));
  }

}