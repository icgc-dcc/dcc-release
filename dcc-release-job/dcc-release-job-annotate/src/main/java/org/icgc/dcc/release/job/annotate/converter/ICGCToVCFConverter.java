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
package org.icgc.dcc.release.job.annotate.converter;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;

import java.util.List;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import net.sf.picard.reference.IndexedFastaSequenceFile;

import org.broadinstitute.variant.variantcontext.Allele;
import org.broadinstitute.variant.variantcontext.GenotypeBuilder;
import org.broadinstitute.variant.variantcontext.GenotypesContext;
import org.icgc.dcc.release.job.annotate.converter.SnpEffVCFToICGCConverter.SampleType;

import com.google.common.collect.Lists;

/**
 * Converts from ICGC mutations to VCF compatible mutations.
 * 
 * @see https://wiki.oicr.on.ca/display/DCCSOFT/Aggregated+Data+Download+Specification
 */
public class ICGCToVCFConverter {

  private static final String MUTATION_FORMAT = "%s>%s";
  private static final int MUTATION_TO_INDEX = 1;
  private static final int MUTATION_FROM_INDEX = 0;

  /**
   * Separator between mutation from and to.
   */
  private static final String MUTATION_PART_SEPARATOR = ">";

  private final IndexedFastaSequenceFile sequenceFile;

  public ICGCToVCFConverter(IndexedFastaSequenceFile sequenceFile) {
    this.sequenceFile = sequenceFile;
  }

  public VCFMutation convert(String chromosome, long start, long end, String mutation, MutationType type,
      String reference) {
    val mutationParts = mutation.split(MUTATION_PART_SEPARATOR);
    val mutationFrom = mutationParts[MUTATION_FROM_INDEX];
    val mutationTo = mutationParts[MUTATION_TO_INDEX];
    val result = new VCFMutation();

    if (type == MutationType.INSERTION) {

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

      result.mutation = String.format(MUTATION_FORMAT, result.ref, result.alt.get(0));
      // TODO: check what to do when start == 1
      // Prepend reference allele
      result.genotype = createSubstitutionGenotype(result.ref, result.ref + mutationTo);
    } else if (type == MutationType.DELETION) {

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

      result.mutation = String.format(MUTATION_FORMAT, result.ref, result.alt.get(0));
      // mutationTo must be part of the reference allele
      result.genotype = createSubstitutionGenotype(result.ref, result.alt.get(0));
    } else if (type == MutationType.MUTLTIPLE_BASE_SUBSTITUTION || type == MutationType.SINGLE_BASE_SUBSTITUTION) {

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
      result.genotype = createSubstitutionGenotype(mutationFrom, mutationTo, result.ref);
    } else {
      checkState(false, "Unexpected mutation type %s", type);
    }

    return result;
  }

  private static GenotypesContext createSubstitutionGenotype(String fromAllele, String toAllele) {
    val fieldsCount = 2;
    val referenceAllele = true;
    val genotypeContext = GenotypesContext.create(fieldsCount);
    genotypeContext.add(GenotypeBuilder.create(SampleType.CONTROL_SAMPLE.getName(),
        getGenotypeAlleles(fromAllele, referenceAllele)));
    genotypeContext.add(GenotypeBuilder.create(SampleType.DONOR_SAMPLE.getName(),
        getGenotypeAlleles(toAllele, !referenceAllele)));

    return genotypeContext;
  }

  private static GenotypesContext createSubstitutionGenotype(String fromAllele, String toAllele, String reference) {
    val fieldsCount = 2;
    val referenceAllele = true;
    val genotypeContext = GenotypesContext.create(fieldsCount);
    val refAllele = Allele.create(reference, referenceAllele);
    genotypeContext.add(GenotypeBuilder.create(SampleType.CONTROL_SAMPLE.getName(),
        getGenotypeAlleles(fromAllele, refAllele.basesMatch(fromAllele))));
    genotypeContext.add(GenotypeBuilder.create(SampleType.DONOR_SAMPLE.getName(),
        getGenotypeAlleles(toAllele, refAllele.basesMatch(toAllele))));

    return genotypeContext;
  }

  private static List<Allele> getGenotypeAlleles(String allele, boolean referenceAllele) {
    List<Allele> result = Lists.newArrayList();
    // We always have homozygous genotypes that's why left part == right part of the genotype. E.g. A/A
    result.add(Allele.create(allele, referenceAllele));
    result.add(Allele.create(allele, referenceAllele));

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

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  public static class VCFMutation {

    public long pos;
    public List<String> alt = newArrayList();
    public String ref;
    public String mutation;
    public GenotypesContext genotype;

  }

  @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
  public static enum MutationType {

    SINGLE_BASE_SUBSTITUTION("single base substitution"),
    INSERTION("insertion of <=200bp"),
    DELETION("deletion of <=200bp"),
    MUTLTIPLE_BASE_SUBSTITUTION("multiple base substitution (>=2bp and <=200bp)");

    @Getter
    private final String id;

    public static MutationType fromId(@NonNull String id) {
      for (val value : values()) {
        // See {@code #normalize}
        if (normalize(value.getId()).equals(normalize(id))) {
          return value;
        }
      }

      throw new IllegalArgumentException("Invalid mutation type id '" + id + "'");
    }

    /**
     * Normalization is required since SGV and SSM are slightly different in whitespace.
     * 
     * <pre>
     * sgv_p.0.variant_type.v1 
     * 1 single base substitution
     * 2 insertion of <= 200 bp
     * 3 deletion of <= 200 bp
     * 4 multiple base substitution (>= 2bp and <= 200bp)
     * 
     * ssm_p.0.mutation_type.v1 
     * 1 single base substitution
     * 2 insertion of <=200bp
     * 3 deletion of <=200bp
     * 4 multiple base substitution (>=2bp and <=200bp)
     * </pre>
     */
    private static String normalize(String value) {
      if (value == null) {
        return null;
      }

      return value.replaceAll(" ", "");
    }

  }

}