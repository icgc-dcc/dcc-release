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
package org.icgc.dcc.etl2.job.annotate.converter;

import static com.google.common.base.Strings.isNullOrEmpty;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.model.SpecialValue.NO_VALUE;
import static org.icgc.dcc.etl2.job.annotate.model.ParseNotification.WARNING_REF_DOES_NOT_MATCH_GENOME;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.broadinstitute.variant.variantcontext.Genotype;
import org.broadinstitute.variant.variantcontext.VariantContext;
import org.icgc.dcc.etl2.job.annotate.model.AnnotatedFileType;
import org.icgc.dcc.etl2.job.annotate.model.SecondaryEntity;
import org.icgc.dcc.etl2.job.annotate.model.SnpEffect;
import org.icgc.dcc.etl2.job.annotate.parser.SnpEffectParser;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

/**
 * Converts snpEff annotated variants to a list of {@link SecondaryEntity}
 */
@Slf4j
@RequiredArgsConstructor
public class SnpEffVCFToICGCConverter {

  public static final String INFO_EFF_FIELD = "EFF";
  private static final String INFO_PRIM_FIELD = "PRIM";

  /**
   * proteinDomainAffected does not have data to be populated. See SsmSecondaryRecord class for more details
   */
  private static final String PROTEIN_AFFECTED_DOMAIN_VALUE = null;
  private static final String MISSING_DATA = (String) NO_VALUE;
  private static final String SSM_NOTE = MISSING_DATA;

  @NonNull
  private final String geneBuildVersion;

  public List<SecondaryEntity> convert(VariantContext variant, AnnotatedFileType fileType) {
    if (!variant.hasAttribute(INFO_PRIM_FIELD)) {
      log.warn("The unique identificator is missing in variant {}", variant);

      return Collections.emptyList();
    }

    val effects = retrieveUniqueCancerEffects(variant);

    return createSecondaryEntities(effects, geneBuildVersion, variant);
  }

  private static List<SecondaryEntity> createSecondaryEntities(Collection<SnpEffect> effects, String geneBuildVersion,
      VariantContext variant) {
    val result = new ImmutableList.Builder<SecondaryEntity>();

    for (val effect : effects) {
      result.add(createSecondaryEntity(effect, geneBuildVersion,
          getObservationId(variant.getAttribute(INFO_PRIM_FIELD))));
    }

    return result.build();
  }

  private static String getObservationId(Object observationId) {
    @SuppressWarnings("unchecked")
    val observationIdList =
        (observationId instanceof List) ? (List<String>) observationId : Arrays.asList((String) observationId);

    return observationIdList.get(0);
  }

  private static SecondaryEntity createSecondaryEntity(SnpEffect effect, String genBuildVersion, String id) {
    return SecondaryEntity.builder()
        .consequenceType(getValue(effect.getConsequenceType().getConsequenceName()))
        .aaMutation(getValue(effect.getAminoAcidChange()))
        .cdsMutation(getValue(effect.getCodonChange()))
        .proteinDomainAffected(getValue(PROTEIN_AFFECTED_DOMAIN_VALUE))
        .geneAffected(getValue(effect.getGeneName()))
        .transcriptAffected(getValue(effect.getTranscriptID()))
        .geneBuildVersion(genBuildVersion)
        .note(SSM_NOTE)
        .observationId(id)
        .build();
  }

  private static String getValue(String input) {
    return (isNullOrEmpty(input)) ? MISSING_DATA : input;
  }

  /**
   * Parses {@code variant} and returns a collections of unique effects sorted by priority and limited by the most
   * important.
   */
  private static Collection<SnpEffect> retrieveUniqueCancerEffects(VariantContext variant) {
    log.debug("Processing variant: {}", variant);
    if (!variant.hasAttribute(INFO_EFF_FIELD)) {
      log.warn("No snpEff annotation found in variant. Skipping. Variant: {}", variant);

      return Collections.emptyList();
    }

    val effects = extractEffects(variant);
    val result = new ImmutableList.Builder<SnpEffect>();

    for (val effectAnnotation : effects) {
      result.addAll(filterMalformedEffects(SnpEffectParser.parse(effectAnnotation), variant));
    }

    return filterEffects(variant, result.build());
  }

  /**
   * Filters out malformed {@link SnpEffect}s.
   * 
   * @return a list of valid effects
   */
  private static List<SnpEffect> filterMalformedEffects(List<SnpEffect> effects, VariantContext variant) {
    val result = new ImmutableList.Builder<SnpEffect>();

    for (val effect : effects) {
      if (isValidEffect(effect, variant)) {
        result.add(effect);
      }
    }

    return result.build();
  }

  private static boolean isValidEffect(SnpEffect effect, VariantContext variant) {
    if (effect.hasError()) {
      // FIXME: [DCC-2578] Confirm with Junjun when a malformed effect can be still added to a ssm_s.txt.
      if (effect.containsAnyError(WARNING_REF_DOES_NOT_MATCH_GENOME)) {
        log.error("Skipping malformed effect: '{}'", effect);

        return false;
      }
      // TODO: Change to other level once this information is required for data mining etc.
      log.debug("Adding effect with warning or error: '{}'", effect);
    }

    return true;
  }

  /**
   * Retrieves all annotated variants from the INFO field. Returns as a list of individual annotations. Each list entity
   * is an unparsed (not broken down to fields) snpEff annotation.
   */
  @SuppressWarnings("unchecked")
  private static List<String> extractEffects(VariantContext variant) {
    val allEffects = variant.getAttribute(INFO_EFF_FIELD);

    return (allEffects instanceof List) ? (List<String>) allEffects : Arrays.asList((String) allEffects);
  }

  /**
   * Groups annotated effects at the transcript or gene level if the transcript one is not available. Selects one effect
   * from the group based on the {@link ConsequenceType}<br>
   * <br>
   * Skips variations which are not not mutations. E.g. controlSample equal to tumourSample (same nucleotide mutated
   * into the same nucleotide).
   * @param variant - annotated variant
   * @param individualEffects - effects retrieved from the {@code vc}
   * @return effects sorted by priority and limited by the most important
   * @see <a href="https://wiki.oicr.on.ca/x/sg6RAw">Effect selection criterias</a>
   */
  private static Collection<SnpEffect> filterEffects(VariantContext variant, List<SnpEffect> individualEffects) {
    val controlGenotype = parseGenotype(variant, SampleType.CONTROL_SAMPLE.getName());
    val tumourGenotype = parseGenotype(variant, SampleType.DONOR_SAMPLE.getName());

    if (!isMutation(controlGenotype, tumourGenotype)) {
      log.warn("No mutations found based on the genotype info. Variant: {}", variant);

      return Collections.emptyList();
    }

    val predictedGenotypeNumber = predictGenotypeNumber(controlGenotype, tumourGenotype);
    val uniqueEffects = filterMatchingEffects(individualEffects, predictedGenotypeNumber);
    val transcriptIdMap = groupEffects(uniqueEffects);
    val result = getFilteredEffects(transcriptIdMap);

    if (result.isEmpty()) {
      log.warn("No effect found for this mutation. Skipping. Variant: {}", variant);
    }

    return result;
  }

  /**
   * Checks if {@code mutationFrom} and {@code mutationTo} represent a mutation.<br>
   * <br>
   * It is not a mutation if both arguments the same. There is no mutation in representation A>A. Must be something like
   * A>G. What means allele A mutated to allele G.
   */
  // TODO: move this to ssm_s to vcf generation class. It's more efficient to check this before the annotation step.
  private static boolean isMutation(String mutationFrom, String mutationTo) {
    return !mutationFrom.equals(mutationTo);
  }

  /**
   * Gets genotype (control or sample) from {@code variant} by {@code genotypeName}. Transforms its name to numeric
   * format which is generated by snpEff. The genotype number corresponds to the allele position in REF or ALT fields in
   * the VCF file. The method returns a single position number of this genotype.
   * 
   * We know that left and right sides of a genotype are always equal, because this is enforced by the submission
   * validator and it looks like "0/0  1/1". So, the method will return either "0" or "1" depending on the
   * {@code genotypeName}
   */
  private static String parseGenotype(VariantContext variant, String genotypeName) {
    val genotype = variant.getGenotype(genotypeName);
    checkPhasing(genotype);

    return transformGenotypeNotion(variant, genotype).get(0);
  }

  /**
   * Converts {@code genotype}, which looks like [T/T] to VCF format. E.g. 1/1
   */
  private static List<String> transformGenotypeNotion(VariantContext variant, Genotype genotype) {
    val result = new ImmutableList.Builder<String>();
    val leftAlleleIndex = 0;
    val rightAlleleIndex = 1;
    result.add(String.valueOf(variant.getAlleleIndex(genotype.getAllele(leftAlleleIndex))));
    result.add(String.valueOf(variant.getAlleleIndex(genotype.getAllele(rightAlleleIndex))));

    return result.build();
  }

  private static void checkPhasing(Genotype genotype) {
    if (genotype.isPhased()) {
      throw new UnsupportedOperationException("Phased genotype is not currently supported. Genotype: " + genotype);
    }
  }

  /**
   * Returns a set of effects sorted by priority and limited by 1
   */
  private static Set<SnpEffect> getFilteredEffects(Multimap<String, SnpEffect> transcriptIdMap) {
    val result = new ImmutableSet.Builder<SnpEffect>();
    for (val transcriptId : transcriptIdMap.keySet()) {
      val effectGroup = Lists.newArrayList(transcriptIdMap.get(transcriptId));
      // order by importance
      Collections.sort(effectGroup, Collections.reverseOrder());
      // limit by 1
      result.add(effectGroup.get(0));
    }

    return result.build();
  }

  /**
   * Groups {@code effects} by transcript or by gene affected if transcript is unavailable.
   */
  private static Multimap<String, SnpEffect> groupEffects(Collection<SnpEffect> effects) {
    Multimap<String, SnpEffect> transcriptIdMap = Multimaps.index(
        effects, new Function<SnpEffect, String>() {

          @Override
          public String apply(SnpEffect item) {
            return !item.getTranscriptID().isEmpty() ? item.getTranscriptID() : item.getGeneName();
          }

        });

    return transcriptIdMap;
  }

  /**
   * Filters {@code individualEffects}. Returns only those that whose {@code cancerID} matches {@code mutationSet}
   */
  private static Collection<SnpEffect> filterMatchingEffects(List<SnpEffect> individualEffects, final String mutationSet) {
    Collection<SnpEffect> uniqueEffects = Collections2.filter(
        individualEffects, new Predicate<SnpEffect>() {

          @Override
          public boolean apply(SnpEffect effect) {
            if (mutationSet.equals(effect.getCancerID())) {
              return true;
            }

            return false;
          }

        });
    return uniqueEffects;
  }

  /**
   * Predicts what Genotype_Number should be valid for the {@code control} / {@code tumour} combination
   */
  private static String predictGenotypeNumber(String control, String tumour) {
    val separator = "-";
    val referenceAllele = "0";

    if (control.equals(referenceAllele)) {
      return tumour;
    } else {
      return tumour + separator + control;
    }
  }

  @Getter
  @RequiredArgsConstructor(access = PRIVATE)
  public static enum SampleType {
    CONTROL_SAMPLE("Patient_01_Germline"),
    DONOR_SAMPLE("Patient_01_Somatic");

    @NonNull
    private final String name;

  }

}