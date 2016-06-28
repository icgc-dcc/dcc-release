/*
 * Copyright (c) 2015 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.release.job.summarize.function;

import static com.google.common.primitives.Ints.tryParse;
import static java.lang.String.format;
import static org.icgc.dcc.common.core.json.Jackson.to;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_AGE_AT_DIAGNOSIS;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SAMPLE;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SAMPLE_SEQUENCE_DATA;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SAMPLE_STUDY;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SPECIMEN;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SUMMARY_AGE_AT_DIAGNOSIS_GROUP;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SUMMARY_EXPERIMENTAL_ANALYSIS;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SUMMARY_EXPERIMENTAL_ANALYSIS_SAMPLE_COUNTS;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SUMMARY_REPOSITORY;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SUMMARY_STUDIES;
import static org.icgc.dcc.common.core.model.FieldNames.SEQUENCE_DATA_LIBRARY_STRATEGY;
import static org.icgc.dcc.common.core.model.FieldNames.SEQUENCE_DATA_REPOSITORY;
import static org.icgc.dcc.release.core.function.Unwind.unwind;
import static org.icgc.dcc.release.core.util.ObjectNodes.createObject;
import static org.icgc.dcc.release.core.util.ObjectNodes.textValue;

import java.util.Map;
import java.util.Set;

import lombok.val;

import org.apache.spark.api.java.function.Function;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public final class CreateDonorSummary implements Function<ObjectNode, ObjectNode> {

  private static final int MIN_DONOR_AGE = 0;

  @Override
  public ObjectNode call(ObjectNode row) throws Exception {
    val resultNode = createObject();
    val summaryNode = resultNode.with(DONOR_SUMMARY);

    summarizeDonorRepositories(row, summaryNode);
    summarizeDonorStudies(row, summaryNode);
    summarizeDonorLibraryStrategies(row, summaryNode);
    summarizeDonorAgeGroups(row, summaryNode);

    return resultNode;
  }

  private static void summarizeDonorStudies(ObjectNode row, ObjectNode summaryNode) throws Exception {
    val donorStudies = getDonorStudies(row);
    summaryNode.putPOJO(DONOR_SUMMARY_STUDIES, to(donorStudies));
  }

  private static Set<String> getDonorStudies(ObjectNode row) throws Exception {
    val donorStudies = Sets.<String> newTreeSet();
    val unwindPath = DONOR_SPECIMEN + "." + DONOR_SAMPLE;
    for (val sample : unwind(unwindPath).call(row)) {
      val study = textValue(sample, DONOR_SAMPLE_STUDY);
      if (study != null) {
        donorStudies.add(study);
      }
    }

    return donorStudies;
  }

  private static void summarizeDonorRepositories(ObjectNode row, ObjectNode summaryNode) throws Exception {
    val donorRepos = getDonorRepositories(row);
    summaryNode.putPOJO(DONOR_SUMMARY_REPOSITORY, to(donorRepos));
  }

  private static Set<String> getDonorRepositories(ObjectNode row) throws Exception {
    val donorRepos = Sets.<String> newTreeSet();
    val unwindPath = DONOR_SPECIMEN + "." + DONOR_SAMPLE + "." + DONOR_SAMPLE_SEQUENCE_DATA;

    for (val sequenceData : unwind(unwindPath).call(row)) {
      val repository = textValue(sequenceData, SEQUENCE_DATA_REPOSITORY);
      if (repository != null) {
        donorRepos.add(repository);
      }
    }

    return donorRepos;
  }

  private static void summarizeDonorLibraryStrategies(ObjectNode row, ObjectNode summaryNode) throws Exception {
    val libraryStrategies = createLibraryStrategies(row);
    summaryNode.putPOJO(DONOR_SUMMARY_EXPERIMENTAL_ANALYSIS, to(libraryStrategies.keySet()));

    val experimentalAnalysisCount = summaryNode.with(DONOR_SUMMARY_EXPERIMENTAL_ANALYSIS_SAMPLE_COUNTS);
    for (val entry : libraryStrategies.entrySet()) {
      experimentalAnalysisCount.put(entry.getKey(), entry.getValue());
    }
  }

  private static Map<String, Integer> createLibraryStrategies(ObjectNode row) throws Exception {
    val samples = unwind(DONOR_SPECIMEN + "." + DONOR_SAMPLE).call(row);
    Map<String, Integer> libStrategies = Maps.newHashMap();
    for (val sample : samples) {
      val sampleLibStrategies = getSampleLibraryStrategies(sample);
      libStrategies = mergeLibraryStrategies(libStrategies, sampleLibStrategies);
    }

    return libStrategies;
  }

  private static Map<String, Integer> mergeLibraryStrategies(Map<String, Integer> map1, Map<String, Integer> map2) {
    val result = Maps.newHashMap(map1);
    for (val entry : map2.entrySet()) {
      val map1Value = map1.get(entry.getKey());
      if (map1Value != null) {
        result.put(entry.getKey(), map1Value + entry.getValue());
      } else {
        result.put(entry.getKey(), entry.getValue());
      }
    }

    return result;
  }

  private static Map<String, Integer> getSampleLibraryStrategies(ObjectNode sample) {
    val sampleLibStrategies = Maps.<String, Integer> newHashMap();
    for (val sampleSequenceData : sample.path(DONOR_SAMPLE_SEQUENCE_DATA)) {
      val libStrategy = textValue(sampleSequenceData, SEQUENCE_DATA_LIBRARY_STRATEGY);
      sampleLibStrategies.put(libStrategy, 1);
    }

    return sampleLibStrategies;
  }

  private static void summarizeDonorAgeGroups(ObjectNode row, ObjectNode summaryNode) {
    val donorAgeAtDiagnosis = textValue(row, DONOR_AGE_AT_DIAGNOSIS);
    val ageGroup = getAgeGroup(donorAgeAtDiagnosis);
    summaryNode.put(DONOR_SUMMARY_AGE_AT_DIAGNOSIS_GROUP, ageGroup);
  }

  private static String getAgeGroup(String age) {
    Integer value = age == null ? null : tryParse(age);

    String ageGroup = null;
    if (value != null) {
      val interval = 10;

      // Produce values of the form: "1 - 9", "10 - 19", ...
      int groupStart = (value / interval) * interval;
      int groupEnd = groupStart + interval - 1;
      ageGroup = formatAgeGroup(groupStart == MIN_DONOR_AGE ? 1 : groupStart, groupEnd);
    }

    return ageGroup;
  }

  private static String formatAgeGroup(int groupStart, int groupEnd) {
    return format("%s - %s", groupStart, groupEnd);
  }

}