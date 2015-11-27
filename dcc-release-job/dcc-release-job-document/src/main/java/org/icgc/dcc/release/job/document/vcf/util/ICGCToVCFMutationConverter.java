/*
 * Copyright (c) 2014 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.release.job.document.vcf.util;

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.release.job.document.vcf.model.MutationType.DELETION;
import static org.icgc.dcc.release.job.document.vcf.model.MutationType.INSERTION;
import static org.icgc.dcc.release.job.document.vcf.model.MutationType.MUTLTIPLE_BASE_SUBSTITUTION;
import static org.icgc.dcc.release.job.document.vcf.model.MutationType.SINGLE_BASE_SUBSTITUTION;
import lombok.val;
import net.sf.picard.reference.IndexedFastaSequenceFile;

import org.icgc.dcc.release.job.document.vcf.model.MutationType;

/**
 * Converts from ICGC mutations to VCF compatible mutations.
 * 
 * @see https://wiki.oicr.on.ca/display/DCCSOFT/Aggregated+Data+Download+Specification
 */
public class ICGCToVCFMutationConverter {

  /**
   * Separator between mutation from and to.
   */
  private static final String MUTATION_PART_SEPARATOR = ">";

  private final IndexedFastaSequenceFile sequenceFile;

  public ICGCToVCFMutationConverter(IndexedFastaSequenceFile sequenceFile) {
    this.sequenceFile = sequenceFile;
  }

  public VCFMutation convert(String chromosome, long start, long end, String mutation, MutationType type,
      String reference) {
    val mutationParts = mutation.split(MUTATION_PART_SEPARATOR);
    val mutationFrom = mutationParts[0];
    val mutationTo = mutationParts[1];
    val result = new VCFMutation();

    if (type == INSERTION) {

      /*
       * Insertion
       */

      if (start == 1) {
        result.pos = start;
        result.ref = getReference(chromosome, start);
        result.alt.add(mutationTo + result.ref);
      } else {
        result.pos = start - 1;
        result.ref = getReference(chromosome, start - 1);
        result.alt.add(result.ref + mutationTo);
      }

      result.mutation = String.format("%s>%s", result.ref, result.alt.get(0));
    } else if (type == DELETION) {

      /*
       * Deletion
       */

      if (start == 1) {
        result.pos = start;
        result.ref = getReference(chromosome, start, start + mutationFrom.length());
        result.alt.add(getReference(chromosome, start + mutationFrom.length()));
      } else {
        result.pos = start - 1;
        result.ref = getReference(chromosome, start - 1, start - 1 + mutationFrom.length());
        result.alt.add(getReference(chromosome, start - 1));
      }

      result.mutation = String.format("%s>%s", result.ref, result.alt.get(0));
    } else if (type == MUTLTIPLE_BASE_SUBSTITUTION || type == SINGLE_BASE_SUBSTITUTION) {

      /*
       * Substitution
       */

      result.pos = start;
      result.ref = reference;

      if (!mutationFrom.equals(reference)) {
        result.alt.add(mutationFrom);
      }
      if (!mutationTo.equals(reference)) {
        result.alt.add(mutationTo);
      }

      result.mutation = mutation;
    } else {
      checkState(false, "Unexpected mutation type %s", type);
    }

    return result;
  }

  private String getReference(String chromosome, long position) {
    return getReference(chromosome, position, position);
  }

  private String getReference(String chromosome, long start, long end) {
    val sequence = sequenceFile.getSubsequenceAt(chromosome, start, end);
    val text = new String(sequence.getBases());

    return text;
  }

}