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
package org.icgc.dcc.release.job.fathmm.core;

import static com.google.common.base.Objects.firstNonNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.primitives.Ints.tryParse;
import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.regex.Pattern.compile;
import static org.apache.commons.lang3.StringUtils.left;
import static org.apache.commons.lang3.StringUtils.right;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;
import org.icgc.dcc.release.job.fathmm.repository.FathmmRepository;

import com.google.common.collect.Maps;

/**
 * This is a java port for FatHMM using postgresql database
 */
@Slf4j
@RequiredArgsConstructor
public class FathmmPredictor {

  /**
   * Constants.
   */
  private static final String WEIGHT_TYPE = "INHERITED";
  private static final String WARNING_NO_SEQUENCE_FOUND = "No Sequence Record Found";
  private static final Pattern SUBSTITUTION_PATTERN = compile("^[ARNDCEQGHILKMFPSTWYV]\\d+[ARNDCEQGHILKMFPSTWYV]$");
  private static final Comparator<Map<String, Object>> INFORMATION_COMPARATOR = new Comparator<Map<String, Object>>() {

    @Override
    public int compare(final Map<String, Object> a, final Map<String, Object> b) {
      return Double.valueOf(b.get("information").toString()).compareTo(
          (Double.valueOf(a.get("information").toString())));
    }
  };

  /**
   * Dependencies.
   */
  @NonNull
  private final FathmmRepository fathmmRepository;

  public Map<String, String> predict(String translationId, String aaChange) {
    Map<String, String> result = null;
    val cache = fathmmRepository.getFromCache(translationId, aaChange);

    if (cache != null) {
      result = newHashMap();
      val score = cache.get("score");
      if (score != null) {
        result.put("Score", score.toString());
        result.put("Prediction", cache.get("prediction").toString());
      }
    } else {
      result = calculateFATHMM(translationId, aaChange);
      val score = result.get("Score");
      if (!isNullOrEmpty(score)) {
        fathmmRepository.updateCache(translationId, aaChange, score, result.get("Prediction"));
      }
    }

    return result;
  }

  // Calculate prediction for substitutions only via FATHMM database
  private Map<String, String> calculateFATHMM(String translationId, String aaChange) {
    val sequence = fathmmRepository.getSequence(translationId);

    // Check null
    if (sequence == null) {
      return singletonMap("Warning", WARNING_NO_SEQUENCE_FOUND);
    }

    // Check aaChange formats
    if (!isSubstitution(aaChange)) {
      return singletonMap("Warning", "Invalid Substitution Format");
    }

    val substitutionPosition = parseSubstitutionPosition(aaChange);
    val sequenceStr = sequence.get("sequence").toString();
    if (substitutionPosition > sequenceStr.length()) {
      return singletonMap("Warning", "Invalid Substitution Position");
    }

    val fromAminoAcid = aaChange.substring(0, 1);
    val sequenceAminoAcid = sequenceStr.substring(substitutionPosition - 1, substitutionPosition);
    if (!fromAminoAcid.equals(sequenceAminoAcid)) {
      val description = format("Inconsistent Wild-Type Residue (Expected '%s')", sequenceAminoAcid);

      return singletonMap("Warning", description);
    }

    if (left(aaChange, 1).equals(right(aaChange, 1))) {
      return singletonMap("Warning", "Synonymous Mutation");
    }

    val sequenceId = (Integer) sequence.get("id");
    val facade = getResidueProbabilities(sequenceId, substitutionPosition);

    // //////////////////////////////////////////////////////////////////////////////
    // Unweighted domain based prediction
    // //////////////////////////////////////////////////////////////////////////////
    for (Map<String, Object> x : facade) {
      val id = (String) x.get("id");
      val probability = fathmmRepository.getWeight(id, WEIGHT_TYPE);

      if (probability != null) {
        return result(x, probability, aaChange);
      }
    }

    // //////////////////////////////////////////////////////////////////////////////
    // Unweighted non-domain based prediction
    // //////////////////////////////////////////////////////////////////////////////
    val facade2 = fathmmRepository.getUnweightedProbability(sequenceId.toString(), substitutionPosition);
    if (facade2 != null) {
      val probability = fathmmRepository.getWeight((String) facade2.get("id"), WEIGHT_TYPE);
      if (null != probability) {
        return result(facade2, probability, aaChange);
      }
    }

    return emptyMap();
  }

  private List<Map<String, Object>> getResidueProbabilities(int sequenceId, int substitutionPosition) {
    val domainList = fathmmRepository.getDomains(sequenceId, substitutionPosition);
    val facade = new ArrayList<Map<String, Object>>();
    for (val domain : domainList) {
      val start = Integer.parseInt(domain.get("seq_begin").toString());
      val end = Integer.parseInt(domain.get("seq_end").toString());
      val hmmBegin = Integer.parseInt(domain.get("hmm_begin").toString());
      val align = (String) domain.get("align");

      val residue = mapPosition(start, end, hmmBegin, align, substitutionPosition);
      if (residue != null) {
        val probability = fathmmRepository.getProbability((String) domain.get("hmm"), Integer.parseInt(residue));
        if (probability != null) {
          facade.add(probability);
        }
      }
    }

    // Sort facade
    if (facade.size() > 1) {
      Collections.sort(facade, INFORMATION_COMPARATOR);
    }

    return facade;
  }

  private static String mapPosition(int seqStart, int seqEnd, int hmmBegin, String align, int substitution) {
    if (substitution < seqStart || substitution > seqEnd) {
      return null;
    }

    int start = seqStart - 1;
    int end = hmmBegin - 1;
    for (char c : align.toCharArray()) {
      if (Character.isUpperCase(c) || Character.isLowerCase(c)) {
        start++;
      }

      if (Character.isUpperCase(c) || c == '-') {
        end++;
      }

      if (start == substitution && Character.isUpperCase(c)) {
        return String.valueOf(end);
      }
    }

    return null;
  }

  private static Map<String, String> result(Map<String, Object> facade, Map<String, Object> probability,
      String aaChange) {

    float W = Float.parseFloat(facade.get(StringUtils.left(aaChange, 1)).toString());
    float M = Float.parseFloat(facade.get(StringUtils.right(aaChange, 1)).toString());
    float D = Float.parseFloat(probability.get("disease").toString()) + 1.0f;
    float O = Float.parseFloat(probability.get("other").toString()) + 1.0f;

    // The original calculation is in log2(...), this is just to change basis
    double score = Math.log(((1.0 - W) * O) / ((1.0 - M) * D)) / Math.log(2);

    // This is intended as well, the original script rounds before comparing against the threshold
    score = Math.floor(score * 100) / 100;

    val result = Maps.<String, String> newHashMap();
    result.put("HMM", (String) facade.get("id"));
    result.put("Description", (String) facade.get("description"));
    result.put("Position", facade.get("position").toString());
    result.put("W", String.valueOf(W));
    result.put("M", String.valueOf(M));
    result.put("D", String.valueOf(D));
    result.put("O", String.valueOf(O));
    result.put("Score", String.valueOf(score));

    if (score <= -1.5f) {
      result.put("Prediction", "DAMAGING");
    } else {
      result.put("Prediction", "TOLERATED");
    }

    return result;
  }

  private static int parseSubstitutionPosition(String aaChange) {
    val text = aaChange.substring(1, aaChange.length() - 1);
    val position = tryParse(text);

    if (position == null) {
      log.warn("Could not parse substitution from '{}'", aaChange);
    }
    // TODO: When DCC-2467 is addressed, uncomment the following
    // checkArgument(result == null, "Could not parse substitution from '%s'", aaChange);

    // TODO: When DCC-2467 is addressed change to: return result;
    return firstNonNull(position, -1);
  }

  private static boolean isSubstitution(String aaChange) {
    val matcher = SUBSTITUTION_PATTERN.matcher(aaChange);

    return matcher.matches();
  }

}
