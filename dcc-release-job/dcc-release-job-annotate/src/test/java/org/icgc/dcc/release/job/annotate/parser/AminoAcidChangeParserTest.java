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

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.release.job.annotate.parser.AminoAcidChangeParser.parseAminoAcidChange;

import org.junit.Test;

public class AminoAcidChangeParserTest {

  @Test
  public void testParseAminoAcidChange() throws Exception {
    assertThat(parseAminoAcidChange("p.Ser125Leu")).isEqualTo("S125L");

    // Frameshift
    assertThat(parseAminoAcidChange("p.Thr120fs")).isEqualTo("T120fs");

    // Doesn't start with 'p.'
    assertThat(parseAminoAcidChange("n.Thr120fs")).isNull();

    assertThat(parseAminoAcidChange("p.Trp120*")).isEqualTo("W120*");
    assertThat(parseAminoAcidChange("p.Trp120*")).isEqualTo("W120*");
    assertThat(parseAminoAcidChange("p.Ile334_Thr337del")).isEqualTo("I334_T337del");

    // Empty or null
    assertThat(parseAminoAcidChange("")).isNull();
    assertThat(parseAminoAcidChange(null)).isNull();
  }

  @Test(expected = IllegalStateException.class)
  public void testParseAminoAcidChange_malformed() {
    parseAminoAcidChange("p.Ser125Le");
  }

  @Test(expected = IllegalStateException.class)
  public void testParseAminoAcidChange_invalid3letterNotation() {
    parseAminoAcidChange("p.ZZZ125Leu");
  }

}
