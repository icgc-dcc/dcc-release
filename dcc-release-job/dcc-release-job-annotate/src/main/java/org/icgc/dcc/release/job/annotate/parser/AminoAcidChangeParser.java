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
import static java.lang.Long.parseLong;
import static java.util.regex.Pattern.compile;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.release.job.annotate.model.InfoHeaderField.AMINO_ACID_CHANGE_KEY;

import java.util.regex.Pattern;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = PRIVATE)
public final class AminoAcidChangeParser {

  public static final long AMINO_ACID_POSITION_CALCULATION_FAILED = -1L;

  private static final Pattern AMINO_ACID_POSITION_PATTERN = compile("\\d+");
  private static final String FRAMESHIFT_VARIANT_FORMAT = "X%dfs";

  /**
   * Standardize how aminoAcidChange should look like.<br>
   * <b>NB:</b> Must be called after CodonChangeParse.parse() was invoked
   */
  public static String standardize(String aminoAcidChange) {
    if (isNullOrEmpty(aminoAcidChange)) {
      return aminoAcidChange;
    }

    if (Character.isDigit(aminoAcidChange.charAt(aminoAcidChange.length() - 1))) {
      val character = aminoAcidChange.charAt(0);
      if (Character.isLetter(character) || character == '*') {
        return aminoAcidChange + character;
      }
    }

    return aminoAcidChange;
  }

  public static String getAminoAcidChange(String[] metadata) {
    try {
      SnpEffectParser.checkMetadataFormat(metadata);
    } catch (IllegalStateException e) {
      log.warn(e.getMessage());

      return null;
    }

    return metadata[AMINO_ACID_CHANGE_KEY.getFieldIndex()];
  }

  public static String convertToFrameshiftVariant(@NonNull String aminoAcidChange) {
    val aminoAcidPosition = calculateAminoAcidPosition(aminoAcidChange);

    if (aminoAcidPosition == AMINO_ACID_POSITION_CALCULATION_FAILED) {
      return aminoAcidChange;
    }

    if (aminoAcidChange.trim().startsWith("-")) {
      return String.format(FRAMESHIFT_VARIANT_FORMAT, aminoAcidPosition + 1L);
    } else {
      log.warn("Incorrectly formated aa_change encountered: {}", aminoAcidChange);

      return aminoAcidChange;
    }
  }

  public static long calculateAminoAcidPosition(@NonNull String aminoAcidChange) {
    val matcher = AMINO_ACID_POSITION_PATTERN.matcher(aminoAcidChange);
    long aminoAcidPosition = 0L;

    while (matcher.find()) {
      if (aminoAcidPosition == 0L) {
        aminoAcidPosition = parseLong(matcher.group()) - 1;
      } else {
        // Jumps here when aminoAcidChange has at least 2 pattern matches. E.g. R123R123
        return AMINO_ACID_POSITION_CALCULATION_FAILED;
      }
    }

    return aminoAcidPosition;
  }

}
