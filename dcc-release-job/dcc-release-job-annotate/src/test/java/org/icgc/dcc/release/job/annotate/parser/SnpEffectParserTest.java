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
import static org.icgc.dcc.release.job.annotate.parser.SnpEffectParser.parseCodonChange;
import static org.icgc.dcc.release.job.annotate.parser.SnpEffectParser.parseTranscriptId;
import lombok.val;

import org.icgc.dcc.release.job.annotate.model.ConsequenceType;
import org.icgc.dcc.release.job.annotate.model.ParseState;
import org.icgc.dcc.release.job.annotate.model.SnpEffect;
import org.junit.Test;

public class SnpEffectParserTest {

  @Test
  public void testParse() throws Exception {
    val annotation =
        "T|intergenic_region|MODIFIER|KSR1P1-IGKV1OR10-1|ENSG00000229485-ENSG00000237592|intergenic_region|ENSG00000229485-ENSG00000237592|||n.42652889G>T||||||";

    val effects = SnpEffectParser.parse(annotation);
    assertThat(effects).hasSize(1);

    val effect = effects.get(0);
    val expectedEffect = SnpEffect.builder()
        .consequenceType(ConsequenceType.INTERGENIC_REGION)
        .allele("T")
        .parseState(new ParseState())
        .build();
    assertThat(effect).isEqualTo(expectedEffect);
  }

  @Test
  public void testParseCodonChange() throws Exception {
    assertThat(parseCodonChange("c.374C>T")).isEqualTo("374C>T");
    assertThat(parseCodonChange("c.123g>t")).isEqualTo("123g>t");
    assertThat(parseCodonChange("n.42652889G>T")).isNull();
    assertThat(parseCodonChange("n.G>T")).isNull();
    assertThat(parseCodonChange("n.42652889Gc>T")).isNull();
  }

  @Test
  public void testParseTranscriptId() throws Exception {
    assertThat(parseTranscriptId("ENST00000257430.4")).isEqualTo("ENST00000257430");
    assertThat(parseTranscriptId("ENST00000257430")).isEqualTo("ENST00000257430");
  }

}
