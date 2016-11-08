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
package org.icgc.dcc.release.job.annotate.converter;

import static org.icgc.dcc.common.core.util.Separators.DASH;

import java.util.Optional;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.broadinstitute.variant.variantcontext.Allele;
import org.broadinstitute.variant.variantcontext.Genotype;
import org.broadinstitute.variant.variantcontext.VariantContext;
import org.icgc.dcc.release.job.annotate.converter.SnpEffVCFToICGCConverter.SampleType;

@Slf4j
public class SignificantEffectsResolver {

  /**
   * Using {@code variant} predicts which annotations produced by SnpEff are related to cancer by using reference and
   * alternative alleles.
   */
  public static Optional<String> getSignificantGenotype(@NonNull VariantContext variant) {
    val controlGenotype = parseGenotype(variant, SampleType.CONTROL_SAMPLE.getName());
    val tumourGenotype = parseGenotype(variant, SampleType.DONOR_SAMPLE.getName());

    if (!isMutation(controlGenotype, tumourGenotype)) {
      log.debug("No mutations found based on the genotype info. Variant: {}", variant);

      return Optional.empty();
    }

    val refAllele = variant.getReference();

    return Optional.of(predictSignificantAllele(refAllele, controlGenotype, tumourGenotype));
  }

  /**
   * Gets left side of genotype (control or sample) from {@code variant} by {@code genotypeName}. The genotype
   * corresponds to the allele in REF or ALT fields in the VCF file.
   * 
   * We know that left and right sides of a genotype are always equal, because this is enforced by the submission
   * validator and it looks like "0/0  1/1".
   */
  private static Allele parseGenotype(VariantContext variant, String genotypeName) {
    val genotype = variant.getGenotype(genotypeName);
    checkPhasing(genotype);
    val leftAlleleIndex = 0;

    return genotype.getAllele(leftAlleleIndex);
  }

  /**
   * Predicts what Allele should be valid for the {@code control} / {@code tumour} combination
   */
  private static String predictSignificantAllele(Allele reference, Allele control, Allele tumour) {
    return control.equals(reference) ?
        tumour.getDisplayString() :
        tumour.getDisplayString() + DASH + control.getDisplayString();
  }

  private static void checkPhasing(Genotype genotype) {
    if (genotype.isPhased()) {
      throw new UnsupportedOperationException("Phased genotype is not currently supported. Genotype: " + genotype);
    }
  }

  /**
   * Checks if {@code mutationFrom} and {@code mutationTo} represent a mutation.<br>
   * <br>
   * It is not a mutation if both arguments the same. There is no mutation in representation A>A. Must be something like
   * A>G. What means allele A mutated to allele G.
   */
  private static boolean isMutation(Allele mutationFrom, Allele mutationTo) {
    return !mutationFrom.equals(mutationTo);
  }

}
