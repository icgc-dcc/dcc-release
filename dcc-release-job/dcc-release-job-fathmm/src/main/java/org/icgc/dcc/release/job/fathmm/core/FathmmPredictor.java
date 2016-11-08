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
import java.util.Map;
import java.util.regex.Pattern;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;
import org.icgc.dcc.release.job.fathmm.repository.FathmmRepository;

import com.google.common.base.Objects;

/**
 * This is a java port for FatHMM using postgresql database
 */
@Slf4j
public class FathmmPredictor implements Closeable {

  /**
   * Constants.
   */
  private static final String WARNING_NO_SEQUENCE_FOUND = "No Sequence Record Found";
  private static final Pattern SUBSTITUTION_PATTERN = compile("^[ARNDCEQGHILKMFPSTWYV]\\d+[ARNDCEQGHILKMFPSTWYV]$");
  private static final Comparator<Map<String, Object>> INFORMATION_COMPARATOR = new Comparator<Map<String, Object>>() {

    @Override
    public int compare(final Map<String, Object> a, final Map<String, Object> b) {
      return Double.valueOf(b.get("information").toString()).compareTo(
          (Double.valueOf(a.get("information").toString())));
    }
  };

  private final FathmmRepository fathmmRepository;

  public FathmmPredictor(@NonNull FathmmRepository fathmmRepository) {
    this.fathmmRepository = fathmmRepository;
  }

  @Override
  public void close() throws IOException {
  }

  public Map<String, String> predict(String translationId, String aaChange) {
    Map<String, String> result = null;
    fathmmRepository.getFromCache(translationId, aaChange);

    Map<String, Object> cache = fathmmRepository.getFromCache(translationId, aaChange);

    if (cache != null) {
      result = newHashMap();
      if (cache.get("score") != null) {
        result.put("Score", cache.get("score").toString());
        result.put("Prediction", cache.get("prediction").toString());
      }
    } else {
      result = calculateFATHMM(translationId, aaChange, "INHERITED");
      fathmmRepository.updateCache(translationId, aaChange, result.get("Score"), result.get("Prediction"));
    }

    return result;
  }

  // Calculate prediction via FATHMM database
  private Map<String, String> calculateFATHMM(String translationId, String aaChange, String weights) {
    Map<String, String> result = newHashMap();
    int substitution = parseSubstitution(aaChange);
    val facade = new ArrayList<Map<String, Object>>();

    Map<String, Object> sequence = fathmmRepository.getSequence(translationId);

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

    val sequenceId = (Integer) sequence.get("id");
    val domainList = fathmmRepository.getDomains(sequenceId, substitution);

    for (val domain : domainList) {
      int start = Integer.parseInt(domain.get("seq_begin").toString());
      int end = Integer.parseInt(domain.get("seq_end").toString());
      int hmmBegin = Integer.parseInt(domain.get("hmm_begin").toString());
      String align = (String) domain.get("align");

      String residue = mapPosition(start, end, hmmBegin, align, substitution);
      if (null != residue) {

        val probability = fathmmRepository.getProbability((String) domain.get("hmm"), Integer.parseInt(residue));
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

      Map<String, Object> probability = fathmmRepository.getWeight(id, weights);

      if (null != probability) {
        return result(x, probability, aaChange, weights);
      }
    }

    // //////////////////////////////////////////////////////////////////////////////
    // Unweighted non-domain based prediction
    // //////////////////////////////////////////////////////////////////////////////
    Map<String, Object> facade2 = fathmmRepository.getUnweightedProbability(sequenceId.toString(), substitution);

    if (null != facade2) {
      Map<String, Object> probability = fathmmRepository.getWeight((String) facade2.get("id"), weights);

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
    val text = aaChange.substring(1, aaChange.length() - 1);
    val result = tryParse(text);

    if (result == null) {
      log.warn("Could not parse substitution from '{}'", aaChange);
    }
    // TODO: When DCC-2467 is addressed, uncomment the following
    // checkArgument(result == null, "Could not parse substitution from '%s'", aaChange);

    // TODO: When DCC-2467 is addressed change to: return result;
    return Objects.firstNonNull(result, -1);
  }

  private static boolean isSubstitution(String aaChange) {
    val matcher = SUBSTITUTION_PATTERN.matcher(aaChange);

    return matcher.matches();
  }
}
