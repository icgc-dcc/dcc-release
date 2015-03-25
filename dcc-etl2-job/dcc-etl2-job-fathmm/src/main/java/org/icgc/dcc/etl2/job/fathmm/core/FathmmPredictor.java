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

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.primitives.Ints.tryParse;
import static java.util.regex.Pattern.compile;
import static org.apache.commons.lang3.StringUtils.left;
import static org.apache.commons.lang3.StringUtils.right;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import lombok.NonNull;
import lombok.val;

import org.apache.commons.lang3.StringUtils;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Query;

/**
 * This is a java port for FatHMM using postgresql database
 */
public class FathmmPredictor implements Closeable {

  private static final String TOLERATED = "TOLERATED";
  private static final String DAMAGING = "DAMAGING";
  private static final String INHERITED = "INHERITED";
  private static final String PREDICTION = "Prediction";
  private static final String SCORE = "Score";
  private static final String AA_MUTATION = "aaMutation";
  private static final String TRANSLATION_ID = "translationId";

  /**
   * Constants.
   */
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
    Map<String, String> result = newHashMap();
    val cache = cacheQuery.bind(TRANSLATION_ID, translationId).bind(AA_MUTATION, aaChange).first();

    if (cache != null) {
      if (cache.get("score") != null) {
        result.put(SCORE, cache.get("score").toString());
        result.put(PREDICTION, cache.get("prediction").toString());
      }
    } else {
      result = calculateFATHMM(translationId, aaChange, INHERITED);
      updateCache(translationId, aaChange, result.get(SCORE), result.get(PREDICTION));
    }

    return result;
  }

  private void updateCache(String translationId, String aaChange, String score, String prediction) {
    handle.execute("insert into \"DCC_CACHE\" (translation_id,  aa_mutation, score, prediction) values (?,?,?,?)",
        translationId, aaChange, score, prediction);
  }

  // Calculate prediction via FATHMM database
  private Map<String, String> calculateFATHMM(String translationId, String aaChange, String weights) {
    val sequence = sequenceQuery.bind(TRANSLATION_ID, translationId).first();
    val substitution = getSubstitution(aaChange);

    // Check the values and return a warning result if any issues are found.
    val preconditionCheckResult = check(sequence, aaChange);
    if (preconditionCheckResult != null) {
      return preconditionCheckResult;
    }

    val facade = new ArrayList<Map<String, Object>>();
    val weightQuery =
        handle.createQuery("select disease, other from \"WEIGHTS\" where id=:wid  and type='" + weights + "'");

    val domainList = domainQuery.bind("sequenceId", sequence.get("id")).bind("substitution", substitution).list();

    for (val domain : domainList) {
      addProbability(facade, substitution, domain);
    }

    // Sort facade
    if (facade.size() > 1) {
      Collections.sort(facade, INFORMATION_COMPARATOR);
    }

    // Phenotypes????

    // //////////////////////////////////////////////////////////////////////////////
    // Unweighted domain based prediction
    // //////////////////////////////////////////////////////////////////////////////
    for (val x : facade) {
      String id = (String) (x.get("id"));
      val probability = weightQuery.bind("wid", id).first();

      if (null != probability) {
        return result(x, probability, aaChange, weights);
      }
    }

    // //////////////////////////////////////////////////////////////////////////////
    // Unweighted non-domain based prediction
    // //////////////////////////////////////////////////////////////////////////////
    val facade2 = unweightedProbabilityQuery
        .bind("sequenceId", sequence.get("id").toString())
        .bind("substitution", substitution).first();

    if (null != facade2) {
      val probability = weightQuery.bind("wid", facade2.get("id")).first();

      if (null != probability) {
        return result(facade2, probability, aaChange, weights);
      }
    }
    return newHashMap();
  }

  /**
   * @param aaChange
   * @return
   */
  private java.lang.Integer getSubstitution(String aaChange) {
    val digits = aaChange.substring(1, aaChange.length() - 1);
    val substitution = tryParse(digits);
    return substitution;
  }

  private void addProbability(final List<Map<String, Object>> facade, final Integer substitution,
      final Map<String, Object> domain) {
    int start = Integer.parseInt(domain.get("seq_begin").toString());
    int end = Integer.parseInt(domain.get("seq_end").toString());
    int hmmBegin = Integer.parseInt(domain.get("hmm_begin").toString());
    String align = (String) domain.get("align");

    val residue = mapPosition(start, end, hmmBegin, align, substitution);
    if (residue.isPresent()) {
      val probability = probabilityQuery
          .bind("hmm", domain.get("hmm"))
          .bind("residue", residue.get()).first();
      if (null != probability) {
        facade.add(probability);
      }
    }
  }

  private static Optional<Integer> mapPosition(int seqStart, int seqEnd, int hmmBegin, String align, int substitution) {
    if (substitution < seqStart || substitution > seqEnd) return Optional.empty();
    int start = seqStart - 1;
    int end = hmmBegin - 1;

    for (char c : align.toCharArray()) {
      if (Character.isUpperCase(c) || Character.isLowerCase(c)) start++;
      if (Character.isUpperCase(c) || c == '-') end++;
      if (start == substitution && Character.isUpperCase(c)) return Optional.of(end);
    }

    return Optional.empty();
  }

  private static Map<String, String> result(Map<String, Object> facade, Map<String, Object> probability,
      String aaChange,
      String weights) {

    float W = Float.parseFloat(facade.get(StringUtils.left(aaChange, 1)).toString());
    float M = Float.parseFloat(facade.get(StringUtils.right(aaChange, 1)).toString());
    float D = Float.parseFloat(probability.get("disease").toString()) + 1.0f;
    float O = Float.parseFloat(probability.get("other").toString()) + 1.0f;

    // The original calculation is in log2(...), this is just to change basis
    double score = Math.log(((1.0 - W) * O) / ((1.0 - M) * D)) / Math.log(2);

    // This is intended as well, the original script rounds before comparing against the threshold
    score = Math.floor(score * 100) / 100;

    Map<String, String> result = newHashMap();
    result.put("HMM", (String) facade.get("id"));
    result.put("Description", (String) facade.get("description"));
    result.put("Position", facade.get("position").toString());
    result.put("W", String.valueOf(W));
    result.put("M", String.valueOf(M));
    result.put("D", String.valueOf(D));
    result.put("O", String.valueOf(O));
    result.put(SCORE, String.valueOf(score));

    if (weights.equals(INHERITED)) {
      if (score <= -1.5f) {
        result.put(PREDICTION, DAMAGING);
      } else {
        result.put(PREDICTION, TOLERATED);
      }
    }
    return result;
  }

  private Map<String, String> check(Map<String, Object> sequence, String aaChange) {

    // Check null
    if (null == sequence) {
      return improperValues("No Sequence Record Found For " + aaChange);
    }

    if (!isSubstitution(aaChange)) {
      return improperValues("Invalid Substitution Format For " + aaChange);
    }

    // The two letters must be different.
    if (left(aaChange, 1).equals(right(aaChange, 1))) {
      return improperValues("Synonymous Mutation For " + aaChange);
    }

    val substitution = getSubstitution(aaChange);

    String substitutionWarning = "Invalid Substitution Value For " + aaChange + " With Substituion: " + substitution;
    if (substitution == null) {
      return improperValues(substitutionWarning);
    }

    // Digit(s) in the middle can not contain only 0(s).
    if (substitution == 0) {
      return improperValues(substitutionWarning);
    }

    // Check aaChange formats
    String sequenceStr = sequence.get("sequence").toString();

    if (substitution > sequenceStr.length()) {
      return improperValues(substitutionWarning);
    }

    val substring = sequenceStr.substring(substitution - 1, substitution);
    if (!aaChange.substring(0, 1).equals(substring)) {
      return improperValues("Inconsistent Wild-Type Residue (Expected '" + substring + "')");
    }
    return null;
  }

  private static boolean isSubstitution(String aaChange) {
    return SUBSTITUTION_PATTERN.matcher(aaChange).matches();
  }

  private static Map<String, String> improperValues(String issue) {
    Map<String, String> result = newHashMap();
    result.put("Warning", issue);
    return result;
  }

}
