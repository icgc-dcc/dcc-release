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
package org.icgc.dcc.release.job.fathmm.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.release.job.fathmm.util.JdbcUrls.FATHMM_JDBC_URL;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

@Slf4j
public class FathmmRepositoryTest {

  FathmmRepository repository = new FathmmRepository(FATHMM_JDBC_URL);

  @Test
  public void testGetProbability() throws Exception {
    val probability = repository.getProbability("PEHE", 36);
    assertThat(probability).hasSize(25);
    assertThat(probability.get("accession")).isEqualTo("PF15275.1");
    assertThat(probability.get("a")).isEqualTo(0.00934952256081);

    assertThat(repository.getProbability("fake", 1)).isNull();
  }

  @Test
  public void testGetDomains() throws Exception {
    val domains = repository.getDomains(6217, 400);

    assertThat(domains).hasSize(2);
    assertThat(domains.get(0)).isEqualTo(ImmutableMap.<String, Object> builder()
        .put("id", 6217)
        .put("hmm", "0050784")
        .put("score", 1.88E-23)
        .put("seq_begin", 390)
        .put("seq_end", 447)
        .put("hmm_begin", 1)
        .put("align", "AFTRSGSFRYHERTHTGEKPYEC--KQCGKAFRSAPNLQLHGR-THTGEKPYQCKECGKAF")
        .build());
    assertThat(domains.get(1)).isEqualTo(ImmutableMap.<String, Object> builder()
        .put("id", 6217)
        .put("hmm", "zf-H2C2_2")
        .put("score", 2.3E-9)
        .put("seq_begin", 397)
        .put("seq_end", 420)
        .put("hmm_begin", 2)
        .put("align", "FRYHERTHTGEKPYECKQCGKAFR")
        .build());
  }

  @Test
  public void testGetUnweightedProbability() throws Exception {
    val probability = repository.getUnweightedProbability("PEHE", 36);
    assertThat(probability).hasSize(25);
    assertThat(probability.get("accession")).isEqualTo("PF15275.1");
    assertThat(probability.get("a")).isEqualTo(0.00934952256081);

    assertThat(repository.getProbability("fake", 1)).isNull();
  }

  @Test
  public void testGetWeight() throws Exception {
    val weight = repository.getWeight("PEHE", "INHERITED");
    log.info("{}", weight);
    assertThat(weight).isNotNull();
  }

}
