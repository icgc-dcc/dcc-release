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

public class AminoAcidConverter {

  private static final Map<String, String> DICTIONARY = ImmutableMap.<String, String> builder()
      .put("Cys", "C")
      .put("Ile", "I")
      .put("Gly", "G")
      .put("Ala", "A")
      .put("Asp", "D")
      .put("Pro", "P")
      .put("His", "H")
      .put("Val", "V")
      .put("Ser", "S")
      .put("Thr", "T")
      .put("Leu", "L")
      .put("Glu", "E")
      .put("Gln", "Q")
      .put("Phe", "F")
      .put("Arg", "R")
      .put("Tyr", "Y")
      .put("Lys", "K")
      .put("Asn", "N")
      .put("Trp", "W")
      .put("Met", "M")
      .build();

  public static String convert(String aminoAcid) {
    return DICTIONARY.get(aminoAcid);
  }

}
