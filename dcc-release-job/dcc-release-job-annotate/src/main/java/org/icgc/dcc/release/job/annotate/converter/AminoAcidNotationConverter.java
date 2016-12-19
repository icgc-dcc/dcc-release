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

import java.util.Map;

import com.google.common.collect.ImmutableMap;

/**
 * Converts 3 letters amino acid representation to 1 letter one.
 * @see https://wiki.oicr.on.ca/display/DCCSOFT/Upgrade+SnpEff+from+v3.6c+to+4.2
 */

public class AminoAcidNotationConverter {

  private static final Map<String, String> DICTIONARY = ImmutableMap.<String, String> builder()
      .put("Cys", "C")
      .put("Asp", "D")
      .put("Ser", "S")
      .put("Gln", "Q")
      .put("Lys", "K")
      .put("Ile", "I")
      .put("Pro", "P")
      .put("Thr", "T")
      .put("Phe", "F")
      .put("Asn", "N")
      .put("Gly", "G")
      .put("His", "H")
      .put("Leu", "L")
      .put("Arg", "R")
      .put("Trp", "W")
      .put("Ala", "A")
      .put("Val", "V")
      .put("Glu", "E")
      .put("Tyr", "Y")
      .put("Met", "M")
      .put("Asx", "B")
      .put("Glx", "Z")
      .put("Ter", "*")
      .build();

  public static String convertLongToShortNotation(String aminoAcid) {
    return DICTIONARY.get(aminoAcid);
  }

}
