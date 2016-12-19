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
package org.icgc.dcc.release.job.annotate.parser;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.Character.isUpperCase;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.release.job.annotate.converter.AminoAcidNotationConverter.convertLongToShortNotation;
import lombok.NoArgsConstructor;
import lombok.val;

/**
 * Specification: https://wiki.oicr.on.ca/display/DCCSOFT/Upgrade+SnpEff+from+v3.6c+to+4.2
 */
@NoArgsConstructor(access = PRIVATE)
public final class AminoAcidChangeParser {

  public static String parseAminoAcidChange(String aaChange) {
    if (isNullOrEmpty(aaChange)) {
      return null;
    }

    // Do not process changes that don't start with 'p.'
    if (!aaChange.startsWith("p.")) {
      return null;
    }

    val result = new StringBuilder();

    // Convert to chars and start after 'p.'
    char[] chars = aaChange.toCharArray();
    for (int i = 2; i < chars.length; i++) {
      val ch = chars[i];
      // Identify the start of the 3-letters amino acid notation
      if (isUpperCase(ch)) {
        checkState(hasEnoughChars(i, chars.length), "Malformed amino acid change. Not enough characters to convert"
            + "the 3 letter amino acid cahnge to the 1 letter one.");

        val threeLetterNotation = charsToString(chars[i], chars[i + 1], chars[i + 2]);
        val oneLetterNotation = convertLongToShortNotation(threeLetterNotation);
        checkState(oneLetterNotation != null, "Failed to convert 3 letter amino acid change notation to the 1 letter "
            + "one: '%s'", threeLetterNotation);
        result.append(oneLetterNotation);

        // Just after the AA notation
        i += 2;
      } else {
        result.append(ch);
      }
    }

    return result.toString();
  }

  private static boolean hasEnoughChars(int i, int length) {
    val endOfNotationIndex = i + 2;

    return endOfNotationIndex <= length - 1;
  }

  private static String charsToString(char... chars) {
    val stringBuilder = new StringBuilder();
    for (val ch : chars) {
      stringBuilder.append(ch);
    }

    return stringBuilder.toString();
  }

}
