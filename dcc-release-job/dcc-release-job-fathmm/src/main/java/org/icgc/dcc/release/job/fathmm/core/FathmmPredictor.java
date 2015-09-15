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
import static org.icgc.dcc.common.core.fi.FathmmImpactCategory.DAMAGING;
import static org.icgc.dcc.common.core.fi.FathmmImpactCategory.TOLERATED;
import static org.icgc.dcc.release.job.fathmm.model.FathmmConstants.INHERITED;
import static org.icgc.dcc.release.job.fathmm.model.FathmmConstants.PREDICTION;
import static org.icgc.dcc.release.job.fathmm.model.FathmmConstants.SCORE;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.commons.lang3.StringUtils;
import org.icgc.dcc.release.job.fathmm.model.FathmmRepository;

import com.google.common.collect.ImmutableMap;

/**
 * This is a java port for FatHMM using postgresql database
 */
@RequiredArgsConstructor
public class FathmmPredictor {

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
  private final FathmmRepository db;

  public Map<String, String> predict(String translationId, String aaChange) {
    val cache = db.getFromCache(translationId, aaChange);

    if (cache != null) {
      if (cache.get("score") != null) {
        return new ImmutableMap.Builder<String, String>()
            .put(SCORE, cache.get("score").toString())
            .put(PREDICTION, cache.get("prediction").toString())
            .build();
      }
    } else {
      val result = calculateFATHMM(translationId, aaChange, INHERITED);
      db.updateCache(translationId, aaChange, result.get(SCORE), result.get(PREDICTION));
      return result;

    }

    return newHashMap();
  }

  // Calculate prediction via FATHMM database
  private Map<String, String> calculateFATHMM(String translationId, String aaChange, String weights) {
    val sequence = db.getSequence(translationId);
    val substitution = getSubstitution(aaChange);

    // Check the values and return a warning result if any issues are found.
    val valueCheckResult = checkValues(sequence, aaChange);
    if (valueCheckResult != null) {
      return valueCheckResult;
    }

    // Phenotypes:
    // Are not being used, so a section of the original FATHMM code has not been ported.
    // See https://github.com/HAShihab/fathmm/blob/master/cgi-bin/fathmm.py#L40 if you're interested.

    // Try Unweighted domain based prediction
    val domainPrediction = calculateDomainPrecition(sequence, substitution, aaChange, weights);
    if (domainPrediction != null) {
      return domainPrediction;
    }

    // Try Unweighted non-domain based prediction
    val nonDomainPrediction = calculateNonDomainPrediction(sequence, substitution, aaChange, weights);
    if (nonDomainPrediction != null) {
      return nonDomainPrediction;
    }

    return newHashMap();
  }

  private Map<String, String> calculateNonDomainPrediction(Map<String, Object> sequence, int substitution,
      String aaChange, String weights) {
    val sequenceId = sequence.get("id").toString();
    val facade = db.getUnweightedProbability(sequenceId, substitution);

    if (null != facade) {
      String id = (String) facade.get("id");
      val probability = db.getWeight(id, weights);

      if (null != probability) {
        return result(facade, probability, aaChange, weights);
      }
    }

    return null;
  }

  private Map<String, String> calculateDomainPrecition(Map<String, Object> sequence, int substitution, String aaChange,
      String weights) {

    val sequenceId = sequence.get("id").toString();
    val domainList = db.getDomains(sequenceId, substitution);
    val facade = calculateaddProbabilities(domainList, substitution);
    Collections.sort(facade, INFORMATION_COMPARATOR);
    for (val x : facade) {
      String id = (String) (x.get("id"));
      val probability = db.getWeight(id, weights);

      if (null != probability) {
        return result(x, probability, aaChange, weights);
      }
    }

    return null;
  }

  private List<Map<String, Object>> calculateaddProbabilities(List<Map<String, Object>> domainList, Integer substitution) {
    val facade = new ArrayList<Map<String, Object>>();
    for (val domain : domainList) {
      facade.addAll(calculateDomainProbabilities(substitution, domain));
    }

    return facade;
  }

  private Integer getSubstitution(String aaChange) {
    val digits = aaChange.substring(1, aaChange.length() - 1);
    val substitution = tryParse(digits);

    return substitution;
  }

  private List<Map<String, Object>> calculateDomainProbabilities(Integer substitution, Map<String, Object> domain) {
    val probabilities = new ArrayList<Map<String, Object>>();
    int start = Integer.parseInt(domain.get("seq_begin").toString());
    int end = Integer.parseInt(domain.get("seq_end").toString());
    int hmmBegin = Integer.parseInt(domain.get("hmm_begin").toString());
    String align = (String) domain.get("align");

    val residue = mapPosition(start, end, hmmBegin, align, substitution);
    if (residue.isPresent()) {
      String hmm = (String) domain.get("hmm");
      val probability = db.getProbability(hmm, residue.get());
      if (null != probability) {
        probabilities.add(probability);
      }
    }

    return probabilities;
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
      String aaChange, String weights) {

    float W = Float.parseFloat(facade.get(StringUtils.left(aaChange, 1)).toString());
    float M = Float.parseFloat(facade.get(StringUtils.right(aaChange, 1)).toString());
    float D = Float.parseFloat(probability.get("disease").toString()) + 1.0f;
    float O = Float.parseFloat(probability.get("other").toString()) + 1.0f;

    // The original calculation is in log2(...), this is just to change basis
    double score = Math.log(((1.0 - W) * O) / ((1.0 - M) * D)) / Math.log(2);

    // This is intended as well, the original script rounds before comparing against the threshold
    score = Math.floor(score * 100) / 100;

    val result = new ImmutableMap.Builder<String, String>();
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
        result.put(PREDICTION, DAMAGING.name());
      } else {
        result.put(PREDICTION, TOLERATED.name());
      }
    }

    return result.build();
  }

  private Map<String, String> checkValues(Map<String, Object> sequence, String aaChange) {

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
