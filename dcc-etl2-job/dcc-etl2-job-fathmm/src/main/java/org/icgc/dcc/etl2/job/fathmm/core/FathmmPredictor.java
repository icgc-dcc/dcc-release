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
package org.icgc.dcc.etl2.job.fathmm.core;

import static com.google.common.collect.Iterables.getFirst;
import static com.google.common.collect.Maps.newHashMap;
import static java.util.regex.Pattern.compile;
import static org.apache.commons.lang3.StringUtils.left;
import static org.apache.commons.lang3.StringUtils.right;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.regex.Pattern;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Query;

import com.google.common.collect.Iterables;

/**
 * This is a java port for FatHMM using postgresql database
 */
@Slf4j
public class FathmmPredictor implements Closeable {

  /**
   * Constants.
   */
  private static final String WARNING_NO_SEQUENCE_FOUND = "No Sequence Record Found";
  private static final Pattern SINGLE_NUCLEOTIDE_PATTERN = compile("^[A-Z]\\d+[A-Z]$");
  private static final Pattern SUBSTITUTION_PATTERN = compile("^[ARNDCEQGHILKMFPSTWYV]\\d+[ARNDCEQGHILKMFPSTWYV]$");
  private static final Comparator<Map<String, Object>> INFORMATION_COMPARATOR = new Comparator<Map<String, Object>>() {

    @Override
    public int compare(final Map<String, Object> a, final Map<String, Object> b) {
      return Double.valueOf(b.get("information").toString()).compareTo(
          (Double.valueOf(a.get("information").toString())));
    }
  };

  @NonNull
  private DBI dbi;
  @NonNull
  private final Handle handle;

  private final Query<Map<String, Object>> cacheQuery;
  private final Query<Map<String, Object>> sequenceQuery;
  private final Query<Map<String, Object>> domainQuery;
  private final Query<Map<String, Object>> probabilityQuery;
  private final Query<Map<String, Object>> unweightedProbabilityQuery;

  public FathmmPredictor(@NonNull String fathmmPostgresqlUri) {
    this.handle = new DBI(fathmmPostgresqlUri).open();

    // @formatter:off
    this.cacheQuery                 = handle.createQuery("select * from \"DCC_CACHE\" where translation_id = :translationId and aa_mutation = :aaMutation");
    this.sequenceQuery              = handle.createQuery("select a.* from \"SEQUENCE\" a, \"PROTEIN\" b where a.id = b.id and b.name = :translationId");
    this.domainQuery                = handle.createQuery("select * from \"DOMAINS\" where id=:sequenceId and :substitution between seq_begin and seq_end order by score");
    this.probabilityQuery           = handle.createQuery("select a.*, b.* from \"PROBABILITIES\" a, \"LIBRARY\" b where a.id=b.id and a.id=:hmm and a.position=:residue");
    this.unweightedProbabilityQuery = handle.createQuery("select a.*, b.* from \"PROBABILITIES\" a, \"LIBRARY\" b where a.id=b.id and a.id=:sequenceId and a.position=:substitution");
    // @formatter:on
  }

  @Override
  public void close() throws IOException {
    handle.close();
  }

  public Map<String, String> predict(String translationId, String aaChange) {
    Map<String, String> result = null;
    Map<String, Object> cache =
        Iterables.getFirst(cacheQuery.bind("translationId", translationId).bind("aaMutation", aaChange).list(), null);

    if (cache != null) {
      result = newHashMap();
      if (cache.get("score") != null) {
        result.put("Score", cache.get("score").toString());
        result.put("Prediction", cache.get("prediction").toString());
      }
    } else {
      result = calculateFATHMM(translationId, aaChange, "INHERITED");
      handle.execute("insert into \"DCC_CACHE\" (translation_id,  aa_mutation, score, prediction) values (?,?,?,?)",
          translationId, aaChange, result.get("Score"), result.get("Prediction"));
    }

    return result;
  }

  // Calculate prediction via FATHMM database
  private Map<String, String> calculateFATHMM(String translationId, String aaChange, String weights) {
    Map<String, String> result = newHashMap();
    int substitution = parseSubstitution(aaChange);
    val facade = new ArrayList<Map<String, Object>>();

    // TODO: Cache this!!!
    val weightQuery =
        handle.createQuery("select * from \"WEIGHTS\" where id=:wid  and type='" + weights + "'");

    Map<String, Object> sequence = getFirst(sequenceQuery.bind("translationId", translationId).list(), null);

    // Check null
    if (null == sequence) {
      result = newHashMap();
      result.put("Warning", WARNING_NO_SEQUENCE_FOUND);
      return result;
    }

    // Check aaChange formats
    String sequenceStr = sequence.get("sequence").toString();

    if (!isSubstitution(aaChange)) {
      result = newHashMap();
      result.put("Warning", "Invalid Substitution Format");
      return result;
    }
    if (substitution > sequenceStr.length()) {
      result = newHashMap();
      result.put("Warning", "Invalid Substitution Position");
      return result;
    }
    if (!aaChange.substring(0, 1).equals(sequenceStr.substring(substitution - 1, substitution))) {
      result = newHashMap();
      result.put(
          "Warning",
          "Inconsistent Wild-Type Residue (Expected '"
              + sequenceStr.substring(substitution - 1, substitution) + "')");
      return result;
    }
    if (left(aaChange, 1).equals(right(aaChange, 1))) {
      result = newHashMap();
      result.put("Warning", "Synonymous Mutation");
      return result;
    }

    val domainList = domainQuery.bind("sequenceId", sequence.get("id")).bind("substitution", substitution).list();

    for (val domain : domainList) {
      int start = Integer.parseInt(domain.get("seq_begin").toString());
      int end = Integer.parseInt(domain.get("seq_end").toString());
      int hmmBegin = Integer.parseInt(domain.get("hmm_begin").toString());
      String align = (String) domain.get("align");

      String residue = mapPosition(start, end, hmmBegin, align, substitution);
      if (null != residue) {
        Map<String, Object> probability =
            Iterables
                .getFirst(probabilityQuery.bind("hmm", domain.get("hmm")).bind("residue", Integer.parseInt(residue))
                    .list(), null);
        if (null != probability) {
          facade.add(probability);
        }
      }
    }

    // Sort facade
    if (facade.size() > 1) {
      Collections.sort(facade, INFORMATION_COMPARATOR);
    }

    // Phenotypes????

    // //////////////////////////////////////////////////////////////////////////////
    // Unweighted domain based prediction
    // //////////////////////////////////////////////////////////////////////////////
    for (Map<String, Object> x : facade) {
      String id = (String) (x.get("id"));
      Map<String, Object> probability = Iterables.getFirst(weightQuery.bind("wid", id).list(), null);

      if (null != probability) {
        return result(x, probability, aaChange, weights);
      }
    }

    // //////////////////////////////////////////////////////////////////////////////
    // Unweighted non-domain based prediction
    // //////////////////////////////////////////////////////////////////////////////
    Map<String, Object> facade2 = Iterables.getFirst(
        unweightedProbabilityQuery
            .bind("sequenceId", sequence.get("id").toString())
            .bind("substitution", substitution).list(), null);

    if (null != facade2) {
      Map<String, Object> probability = Iterables.getFirst(weightQuery.bind("wid", facade2.get("id")).list(), null);

      if (null != probability) {
        return result(facade2, probability, aaChange, weights);
      }
    }
    return newHashMap();
  }

  private static String mapPosition(int seqStart, int seqEnd, int hmmBegin, String align, int substitution) {
    if (substitution < seqStart || substitution > seqEnd) return null;

    int start = seqStart - 1;
    int end = hmmBegin - 1;
    for (char c : align.toCharArray()) {
      if (Character.isUpperCase(c) || Character.isLowerCase(c)) start++;
      if (Character.isUpperCase(c) || c == '-') end++;
      if (start == substitution && Character.isUpperCase(c)) return String.valueOf(end);
    }
    return null;
  }

  private static Map<String, String> result(Map<String, Object> facade, Map<String, Object> probability,
      String aaChange,
      String weights) {

    Map<String, String> result = newHashMap();
    float W = Float.parseFloat(facade.get(StringUtils.left(aaChange, 1)).toString());
    float M = Float.parseFloat(facade.get(StringUtils.right(aaChange, 1)).toString());
    float D = Float.parseFloat(probability.get("disease").toString()) + 1.0f;
    float O = Float.parseFloat(probability.get("other").toString()) + 1.0f;

    // The original calculation is in log2(...), this is just to change basis
    double score = Math.log(((1.0 - W) * O) / ((1.0 - M) * D)) / Math.log(2);

    // This is intended as well, the original script rounds before comparing against the threshold
    score = Math.floor(score * 100) / 100;

    result = newHashMap();
    result.put("HMM", (String) facade.get("id"));
    result.put("Description", (String) facade.get("description"));
    result.put("Position", facade.get("position").toString());
    result.put("W", String.valueOf(W));
    result.put("M", String.valueOf(M));
    result.put("D", String.valueOf(D));
    result.put("O", String.valueOf(O));
    result.put("Score", String.valueOf(score));

    if (weights.equals("INHERITED")) {
      if (score <= -1.5f) {
        result.put("Prediction", "DAMAGING");
      } else {
        result.put("Prediction", "TOLERATED");
      }
    }
    return result;
  }

  private static int parseSubstitution(String aaChange) {
    val matcher = SINGLE_NUCLEOTIDE_PATTERN.matcher(aaChange);
    int result = -1;

    if (matcher.matches()) {
      if (aaChange.charAt(0) == aaChange.charAt(aaChange.length() - 1)) {
        log.warn("Could not parse substitution from '{}', start and end letters must be different.", aaChange);
        return -1;
      }

      val digitsText = aaChange.substring(1, aaChange.length() - 1);

      try {
        result = Integer.parseInt(digitsText);
      } catch (NumberFormatException e) {
        log.warn("Could not parse substitution from '{}'", aaChange);
        return -1;
      }

      if (result == 0) {
        log.warn("Could not parse substitution from '{}', digits can not contain only 0(s);", aaChange);
        return -1;
      }

    }

    return result;
  }

  private static boolean isSubstitution(String aaChange) {
    val matcher = SUBSTITUTION_PATTERN.matcher(aaChange);

    return matcher.matches();
  }

}
