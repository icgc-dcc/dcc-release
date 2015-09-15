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
package org.icgc.dcc.release.job.annotate.parser;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.regex.Pattern.quote;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.release.job.annotate.model.InfoHeaderField.CODON_CHANGE_KEY;
import static org.icgc.dcc.release.job.annotate.parser.AminoAcidChangeParser.AMINO_ACID_POSITION_CALCULATION_FAILED;
import static org.icgc.dcc.release.job.annotate.parser.AminoAcidChangeParser.calculateAminoAcidPosition;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * Parses CodonChange field from annotation produced by snpEff tool. Standardize its representation.
 */
@Slf4j
@NoArgsConstructor(access = PRIVATE)
public final class CodonChangeParser {

  private static final String MUTATION_SEPARATOR = ">";
  private static final String CODON_PAIRS_SEPARATOR = "/";
  private static final int CODON_RIGHT_PAIR = 1;
  private static final int CODONS_QUANTITY = 2;
  private static final int CODON_LEFT_PAIR = 0;
  private static final int CODON_LENGTH = 3;
  private static final String EMPTY_VALUE = "";

  /**
   * Get codonChange from {@code metadata}.
   * @return codonChange or null if {@code metadata} is malformed or null.
   */
  public static String getCodonChange(String[] metadata) {
    try {
      SnpEffectParser.checkMetadataFormat(metadata);
    } catch (IllegalStateException e) {
      log.warn(e.getMessage());

      return null;
    }

    return metadata[CODON_CHANGE_KEY.getFieldIndex()];
  }

  /**
   * <b>NB:</b> If this method is invoked for frameshift_effect {@code aminoAcidChange} must be formatted accordingly
   * See line 228 of SnpEffect class.
   * 
   * @return parsed codonChange.<br>
   * <b>{@code null}</b> if cds_mutation generation failed (this must be handled by updating errorCode, isWellFormed and
   * errorMessage of the SnpEffect).<br>
   * <b>Empty String</b> if {@code codonChange}/ {@code aminoAcidChange} are malformed.
   */
  public static String parseCodonChange(String codonChange, String aminoAcidChange) {
    if (isNullOrEmpty(codonChange) || isNullOrEmpty(aminoAcidChange)) {
      return EMPTY_VALUE;
    }

    val codonPairs = codonChange.split(quote(CODON_PAIRS_SEPARATOR));
    if (!isCorrectCodonPairsFormat(codonPairs)) {
      return EMPTY_VALUE;
    }

    val codonChanges = findCodonChanges(codonPairs);
    if (!codonChanges.isFound()) {
      return null;
    }

    long aminoAcidPosition = calculateAminoAcidPosition(aminoAcidChange);
    if (aminoAcidPosition == AMINO_ACID_POSITION_CALCULATION_FAILED) {
      return EMPTY_VALUE;
    }

    val cdsPosition = aminoAcidPosition * 3L + codonChanges.getBaseOffsetHighlight() + 1L;
    val cdsChange = getCdsChange(codonPairs, codonChanges);

    return cdsPosition + cdsChange;
  }

  private static String getCdsChange(String[] codonPairs, CodonChangeContainer codonChanges) {
    return (getMutatedNucleotides(codonPairs[CODON_LEFT_PAIR], codonChanges) +
        MUTATION_SEPARATOR + getMutatedNucleotides(codonPairs[CODON_RIGHT_PAIR], codonChanges)).toUpperCase();
  }

  private static String getMutatedNucleotides(String codon, CodonChangeContainer codonChanges) {
    return codon.substring(codonChanges.getBaseOffsetHighlight(),
        codonChanges.getBaseOffsetHighlight() + codonChanges.getChangeLength());
  }

  private static boolean isCorrectCodonPairsFormat(String[] codonPairs) {
    if (codonPairs.length != CODONS_QUANTITY || codonPairs[CODON_LEFT_PAIR].length() != CODON_LENGTH
        || codonPairs[CODON_RIGHT_PAIR].length() != CODON_LENGTH) {
      return false;
    }

    return true;
  }

  private static CodonChangeContainer findCodonChanges(String[] codonPair) {
    val result = findChangeInCodon(codonPair[CODON_LEFT_PAIR]);

    return result.isFound() ? result : findChangeInCodon(codonPair[CODON_RIGHT_PAIR]);
  }

  /**
   * Finds changes in a codon.<br>
   * A capital letter represents codon change in particular nucleotide. A codon with all lower case letters does not
   * have changes. E.g. "acg".<br>
   * "Acg" - Start of highligtedBase = 0 (This value is used in String.substring(), so first position starts with 0).
   * Length of change = 1.<br>
   * "ACg" - Start of highligtedBase = 0. Length of change = 2.<br>
   * "gAC" - Start of highligtedBase = 1. Length of change = 2.
   */
  // TODO: What if the codon looks like "AcG". Is this an error?
  private static CodonChangeContainer findChangeInCodon(String codon) {
    val result = new CodonChangeContainer();

    for (int i = 0; i < codon.length(); ++i) {
      if (Character.isUpperCase(codon.charAt(i))) {

        if (!result.isFound()) {
          result.setBaseOffsetHighlight(i);
          result.setFound(true);
        }

        result.incrementChangeLength();
      } else if (result.isFound()) {
        break;
      }
    }

    return result;
  }

  @Data
  private static class CodonChangeContainer {

    private boolean found = false;
    private int baseOffsetHighlight = -1;
    private int changeLength = 0;

    public void incrementChangeLength() {
      changeLength++;
    }

  }

}
