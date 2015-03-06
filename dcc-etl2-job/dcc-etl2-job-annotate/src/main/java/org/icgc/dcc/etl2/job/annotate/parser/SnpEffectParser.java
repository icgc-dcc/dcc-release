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
package org.icgc.dcc.etl2.job.annotate.parser;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.regex.Pattern.quote;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.etl2.job.annotate.model.ConsequenceType.FRAMESHIFT_VARIANT;
import static org.icgc.dcc.etl2.job.annotate.model.ConsequenceType.UNKNOWN_CONSEQUENCE;
import static org.icgc.dcc.etl2.job.annotate.model.ConsequenceType.byId;
import static org.icgc.dcc.etl2.job.annotate.model.EffectFunctionalClass.NONE;
import static org.icgc.dcc.etl2.job.annotate.model.InfoHeaderField.AMINO_ACID_LENGTH_KEY;
import static org.icgc.dcc.etl2.job.annotate.model.InfoHeaderField.CANCER_ID_KEY;
import static org.icgc.dcc.etl2.job.annotate.model.InfoHeaderField.CODING_KEY;
import static org.icgc.dcc.etl2.job.annotate.model.InfoHeaderField.CONSEQUENCE_TYPE_KEY;
import static org.icgc.dcc.etl2.job.annotate.model.InfoHeaderField.EXON_ID_KEY;
import static org.icgc.dcc.etl2.job.annotate.model.InfoHeaderField.FUNCTIONAL_CLASS_KEY;
import static org.icgc.dcc.etl2.job.annotate.model.InfoHeaderField.GENE_BIOTYPE_KEY;
import static org.icgc.dcc.etl2.job.annotate.model.InfoHeaderField.GENE_NAME_KEY;
import static org.icgc.dcc.etl2.job.annotate.model.InfoHeaderField.TRANSCRIPT_ID_KEY;
import static org.icgc.dcc.etl2.job.annotate.model.ParseNotification.CDS_MUTATION_FAILURE;
import static org.icgc.dcc.etl2.job.annotate.model.ParseNotification.MISSING_CONSEQUENCE_TYPE;
import static org.icgc.dcc.etl2.job.annotate.model.ParseNotification.UNKNOWN_FUNCTIONAL_CLASS;
import static org.icgc.dcc.etl2.job.annotate.parser.AminoAcidChangeParser.convertToFrameshiftVariant;
import static org.icgc.dcc.etl2.job.annotate.parser.AminoAcidChangeParser.getAminoAcidChange;
import static org.icgc.dcc.etl2.job.annotate.parser.AminoAcidChangeParser.standardize;
import static org.icgc.dcc.etl2.job.annotate.parser.CodonChangeParser.getCodonChange;
import static org.icgc.dcc.etl2.job.annotate.parser.CodonChangeParser.parseCodonChange;

import java.util.List;

import lombok.NoArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl2.job.annotate.model.EffectFunctionalClass;
import org.icgc.dcc.etl2.job.annotate.model.EffectImpact;
import org.icgc.dcc.etl2.job.annotate.model.ParseNotification;
import org.icgc.dcc.etl2.job.annotate.model.ParseState;
import org.icgc.dcc.etl2.job.annotate.model.SnpEffect;

import com.google.common.collect.ImmutableList;

/**
 * This class orchestrates the set of parsers that parse individual snpEff annotated fields.
 */
@Slf4j
@NoArgsConstructor(access = PRIVATE)
public final class SnpEffectParser {

  /**
   * Number of expected metadata fields annotated by snpEff
   */
  public static final int METADATA_FIELDS_COUNT = 11;

  /**
   * Number of expected metadata fields annotated by snpEff when an error or warning encountered
   */
  public static final int WARNING_OR_ERROR_METADATA_FIELDS_COUNT = 12;

  /**
   * Number of expected metadata fields annotated by snpEff when an error and warning encountered
   */
  public static final int WARNING_AND_ERROR_MEDATA_FIELDS_COUNT = 13;
  public static final SnpEffect MALFORMED_SNP_EFFECT = SnpEffect.builder().consequenceType(UNKNOWN_CONSEQUENCE).build();

  private static final String METADATA_DELIMITER = "[()]";
  private static final String METADATA_SUBFIELD_DELIMITER = "|";
  private static final int EFFECT_NAME_INDEX = 0;
  private static final int EFFECT_METADATA_INDEX = 1;
  private static final String EMPTY_VALUE = "";

  // If there is either a warning OR an error, it will be in the last field. If there is both a warning AND an error,
  // the warning will be in the second-to-last field, and the error will be in the last field.
  private static final int SNPEFF_WARNING_OR_ERROR_FIELD_UPON_SINGLE_ERROR = WARNING_OR_ERROR_METADATA_FIELDS_COUNT - 1;
  private static final int SNPEFF_WARNING_FIELD_UPON_BOTH_WARNING_AND_ERROR = WARNING_AND_ERROR_MEDATA_FIELDS_COUNT - 2;
  private static final int SNPEFF_ERROR_FIELD_UPON_BOTH_WARNING_AND_ERROR = WARNING_AND_ERROR_MEDATA_FIELDS_COUNT - 1;

  /**
   * Check that {@code metadata} array contains is not null and have correct number of fields.
   * 
   * @throws IllegalStateException if {@code metadata} is null or malformed.
   */
  public static void checkMetadataFormat(String[] metadata) {
    checkState(metadata != null, "Metadata array is null");
    checkState(metadata.length >= METADATA_FIELDS_COUNT && metadata.length <= WARNING_AND_ERROR_MEDATA_FIELDS_COUNT,
        "Incorrect size of metadata array: %s", metadata.length);
  }

  /**
   * Parses an annotated by snpEff effect.
   * 
   * @param effectAnnotation - complete effect annotation with name. E.g.
   * {@code exon_variant(MODIFIER|||||ENSG00000000005|processed_transcript|CODING|ENST00000485971|2|1)}
   */
  public static List<SnpEffect> parse(String effectAnnotation) {
    if (isNullOrEmpty(effectAnnotation)) {
      return ImmutableList.of(MALFORMED_SNP_EFFECT);
    }

    val effectNameAndMetadata = effectAnnotation.split(METADATA_DELIMITER);
    val fieldsCount = 2;
    if (effectNameAndMetadata.length != fieldsCount) {
      log.warn("Malformed SnpEff effect: {}", effectAnnotation);

      return ImmutableList.of(MALFORMED_SNP_EFFECT);
    }

    val effectMetadata = effectNameAndMetadata[EFFECT_METADATA_INDEX].split(quote(METADATA_SUBFIELD_DELIMITER), -1);
    try {
      checkMetadataFormat(effectMetadata);
    } catch (IllegalStateException e) {
      log.warn(e.getMessage());

      return ImmutableList.of(MALFORMED_SNP_EFFECT);
    }

    val effectName = effectNameAndMetadata[EFFECT_NAME_INDEX];
    val result = new ImmutableList.Builder<SnpEffect>();
    for (val consequenceType : ConsequenceTypeParser.parse(effectName)) {
      result.add(parseIndividualEffect(consequenceType, effectMetadata));
    }

    return result.build();
  }

  private static SnpEffect parseIndividualEffect(String effectName, String[] effectMetadata) {
    val result = SnpEffect.builder();
    val parseState = new ParseState();

    result.consequenceType(byId(effectName));
    parseWarningsAndErrors(parseState, effectMetadata);
    result.impact(parseImpactField(parseState, effectMetadata));
    result.functionalClass(parseFunctionalClass(parseState, effectMetadata));

    val codonAndAminoAcid = parseCodonChangeAndAminoAcidChange(parseState, effectMetadata, effectName);
    result.codonChange(codonAndAminoAcid.get(0));
    result.aminoAcidChange(codonAndAminoAcid.get(1));

    result.aminoAcidLength(effectMetadata[AMINO_ACID_LENGTH_KEY.getFieldIndex()]);
    result.geneName(effectMetadata[GENE_NAME_KEY.getFieldIndex()]);
    result.geneBiotype(effectMetadata[GENE_BIOTYPE_KEY.getFieldIndex()]);
    result.coding(effectMetadata[CODING_KEY.getFieldIndex()]);
    result.transcriptID(effectMetadata[TRANSCRIPT_ID_KEY.getFieldIndex()]);
    result.exonID(effectMetadata[EXON_ID_KEY.getFieldIndex()]);
    result.cancerID(effectMetadata[CANCER_ID_KEY.getFieldIndex()]);
    result.parseState(parseState);

    return result.build();
  }

  private static List<String> parseCodonChangeAndAminoAcidChange(ParseState error, String[] effectMetadata,
      String effectName) {

    // No need to check if metadata is malformed. That was done in the parent method.
    String codonChange = getCodonChange(effectMetadata);
    String aminoAcidChange = getAminoAcidChange(effectMetadata);

    // Special treatment for frame shift
    if (FRAMESHIFT_VARIANT.getConsequenceName().equals(effectName)) {
      aminoAcidChange = convertToFrameshiftVariant(aminoAcidChange);
    }

    codonChange = parseCodonChange(codonChange, aminoAcidChange);

    if (codonChange == null) {
      error.addErrorAndMessage(CDS_MUTATION_FAILURE, "cds_mutation generation fail");
      codonChange = EMPTY_VALUE;
    }

    // TODO: Why it's not standardized when codonChange is empty?
    if (!codonChange.isEmpty()) {
      aminoAcidChange = standardize(aminoAcidChange);
    }

    return ImmutableList.of(codonChange, aminoAcidChange);
  }

  /**
   * The impact field will never be empty, and should always contain one of the enumerated values
   */
  // Create a ParseResult container which would contain: value, status, errorCode, errorMessage and return it
  // as a result of parsing. This would avoid passing SnpEffect as the method argument
  private static EffectImpact parseImpactField(ParseState error, String[] effectMetadata) {
    try {
      return EffectImpact.valueOf(effectMetadata[CONSEQUENCE_TYPE_KEY.getFieldIndex()]);
    } catch (IllegalArgumentException e) {
      error.addErrorAndMessage(MISSING_CONSEQUENCE_TYPE, "Unrecognized value for effect impact: '%s'",
          effectMetadata[CONSEQUENCE_TYPE_KEY.getFieldIndex()]);

      return EffectImpact.MODIFIER;
    }
  }

  /**
   * The functional class field will be empty when the effect has no functional class associated with it
   */
  private static EffectFunctionalClass parseFunctionalClass(ParseState error, String[] effectMetadata) {
    val functionalClass = effectMetadata[FUNCTIONAL_CLASS_KEY.getFieldIndex()];

    if (!isNullOrEmpty(functionalClass)) {
      try {
        return EffectFunctionalClass.valueOf(functionalClass);
      } catch (IllegalArgumentException e) {
        error.addErrorAndMessage(UNKNOWN_FUNCTIONAL_CLASS, "Unrecognized value for effect functional class: '%s'",
            functionalClass);

        return NONE;
      }
    }

    return NONE;
  }

  /**
   * Checks if the annotation does not contain warnings and errors. If so, update {@code parseError} and
   * {@code errorCode} accordingly.
   */
  private static void parseWarningsAndErrors(ParseState error, String[] effectMetadata) {
    if (effectMetadata.length != METADATA_FIELDS_COUNT) {

      switch (effectMetadata.length) {
      case WARNING_OR_ERROR_METADATA_FIELDS_COUNT:
        List<ParseNotification> parseError =
            ParseNotification.parse(effectMetadata[SNPEFF_WARNING_OR_ERROR_FIELD_UPON_SINGLE_ERROR]);
        error.addAllErrors(parseError);

        break;

      case WARNING_AND_ERROR_MEDATA_FIELDS_COUNT:
        parseError = ParseNotification.parse(effectMetadata[SNPEFF_WARNING_FIELD_UPON_BOTH_WARNING_AND_ERROR]);
        error.addAllErrors(parseError);

        parseError = ParseNotification.parse(effectMetadata[SNPEFF_ERROR_FIELD_UPON_BOTH_WARNING_AND_ERROR]);
        error.addAllErrors(parseError);

        break;
      default:
        log.warn("Wrong number of effect metadata fields. Expected {} but found {}",
            METADATA_FIELDS_COUNT, effectMetadata.length);
        error.addErrorCode(ParseNotification.PARSING_EXCEPTION);
      }

    }
  }

}
