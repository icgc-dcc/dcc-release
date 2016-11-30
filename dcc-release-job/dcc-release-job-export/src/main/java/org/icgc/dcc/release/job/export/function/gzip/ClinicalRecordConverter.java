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
package org.icgc.dcc.release.job.export.function.gzip;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Collections.emptyMap;
import static org.icgc.dcc.common.core.model.DownloadDataType.SAMPLE;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_ID;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SAMPLE;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SAMPLE_STUDY;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SPECIMEN;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SPECIMEN_ID;
import static org.icgc.dcc.common.core.model.FieldNames.PROJECT_ID;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_DONOR_ID;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_SPECIMEN_ID;
import static org.icgc.dcc.common.core.util.Separators.EMPTY_STRING;
import static org.icgc.dcc.common.core.util.Separators.UNDERSCORE;
import static org.icgc.dcc.release.core.util.FieldNames.ExportFieldNames.STUDY_DONOR_INVOLVED_IN;
import static org.icgc.dcc.release.core.util.FieldNames.ExportFieldNames.STUDY_SPECIMEN_INVOLVED_IN;
import static org.icgc.dcc.release.core.util.ObjectNodes.textValue;
import static org.icgc.dcc.release.core.util.Tuples.tuple;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.icgc.dcc.common.core.model.DownloadDataType;
import org.icgc.dcc.common.core.util.Joiners;
import org.icgc.dcc.release.core.function.Unwind;
import org.icgc.dcc.release.core.util.Keys;
import org.icgc.dcc.release.job.export.function.ConvertRow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import lombok.SneakyThrows;
import lombok.val;
import scala.Tuple2;

public class ClinicalRecordConverter implements RecordConverter, PairFlatMapFunction<ObjectNode, String, String> {

  private static final ImmutableList<String> DONOR_FIELDS = ImmutableList.of(DONOR_ID, PROJECT_ID, SUBMISSION_DONOR_ID);
  private static final ConvertRow DONOR_CONVERTER = new ConvertRow(DownloadDataType.DONOR.getDownloadFields());

  @Override
  public JavaPairRDD<String, String> convert(JavaRDD<ObjectNode> input) {
    return input.flatMapToPair(this);
  }

  @Override
  public Iterable<Tuple2<String, String>> call(ObjectNode row) throws Exception {
    val outputRows = ImmutableList.<Tuple2<String, String>> builder();

    val donorKey = getKey(row, DownloadDataType.DONOR);
    val study = resolveStudy(row);
    addDonorStudyInvolvedIn(row, study);
    val donor = DONOR_CONVERTER.call(row);
    outputRows.add(tuple(donorKey, donor));

    outputRows.addAll(convertNestedType(row, DownloadDataType.DONOR_EXPOSURE));
    outputRows.addAll(convertNestedType(row, DownloadDataType.DONOR_FAMILY));
    outputRows.addAll(convertNestedType(row, DownloadDataType.DONOR_THERAPY));
    outputRows.addAll(convertNestedType(row, Optional.of(study), DownloadDataType.SPECIMEN));
    outputRows.addAll(convertSample(row));

    val specimenIds = resolveSpecimenIds(row);
    outputRows.addAll(convertSupplementalDonorSpecimenType(row, DownloadDataType.BIOMARKER, specimenIds));
    outputRows.addAll(convertSupplementalDonorSpecimenType(row, DownloadDataType.SURGERY, specimenIds));

    return outputRows.build();
  }

  private static Map<String, String> resolveSpecimenIds(ObjectNode row) {
    val specimens = row.path(DONOR_SPECIMEN);
    if (specimens.isMissingNode()) {
      return emptyMap();
    }

    val specimenIds = ImmutableMap.<String, String> builder();
    for (val specimen : specimens) {
      val submissionSpecimenId = textValue(specimen, SUBMISSION_SPECIMEN_ID);
      val specimenId = textValue(specimen, DONOR_SPECIMEN_ID);
      if (!isNullOrEmpty(specimenId) && !isNullOrEmpty(submissionSpecimenId)) {
        specimenIds.put(submissionSpecimenId, specimenId);
      }
    }

    return specimenIds.build();
  }

  private static Iterable<Tuple2<String, String>> convertNestedType(ObjectNode row, DownloadDataType dataType) {
    return convertNestedType(row, dataType, Optional.empty(), Optional.empty());
  }

  private static Iterable<Tuple2<String, String>> convertNestedType(ObjectNode row, DownloadDataType dataType,
      Optional<String> keyOpt) {
    return convertNestedType(row, dataType, keyOpt, Optional.empty());
  }

  private static Iterable<Tuple2<String, String>> convertNestedType(ObjectNode row,
      Optional<Multimap<String, String>> study, DownloadDataType dataType) {
    return convertNestedType(row, dataType, Optional.empty(), study);
  }

  @SneakyThrows
  private static Iterable<Tuple2<String, String>> convertNestedType(
      ObjectNode row,
      DownloadDataType dataType,
      Optional<String> keyOpt,
      Optional<Multimap<String, String>> study) {
    val outputRows = ImmutableList.<Tuple2<String, String>> builder();

    val key = keyOpt.isPresent() ? keyOpt.get() : getKey(row, dataType);

    // Fields not available after
    val parentFields = SAMPLE == dataType ? getSpecimenIds(row) : getDonorProjectIds(row);
    val unwindPath = resolveDonorNestedPath(dataType);
    val unwinder = Unwind.unwind(unwindPath);
    val converter = new ConvertRow(dataType.getDownloadFields());

    for (val child : unwinder.call(row)) {
      if (isSpecimen(child, study)) {
        addSpecimenStudyInvolvedIn(child, study.get());
      }
      addParentValues(parentFields, child);
      val value = converter.call(child);
      outputRows.add(tuple(key, value));
    }

    return outputRows.build();
  }

  @SneakyThrows
  private Iterable<Tuple2<String, String>> convertSample(ObjectNode row) {
    val dataType = DownloadDataType.SAMPLE;
    val outputRows = ImmutableList.<Tuple2<String, String>> builder();
    val key = getKey(row, dataType);

    val specimenRetainFields = Lists.newArrayList(DONOR_FIELDS);
    specimenRetainFields.add(DONOR_SPECIMEN);
    val donor = row.deepCopy().retain(specimenRetainFields);
    val donorProject = getDonorProjectIds(donor);
    val unwinder = Unwind.unwind(DONOR_SPECIMEN);
    val keyOpt = Optional.of(key);

    for (val specimen : unwinder.call(donor)) {
      addParentValues(donorProject, specimen);
      outputRows.addAll(convertNestedType(specimen, dataType, keyOpt));
    }

    return outputRows.build();
  }

  @SneakyThrows
  private static Iterable<Tuple2<String, String>> convertSupplementalDonorSpecimenType(ObjectNode row,
      DownloadDataType dataType, Map<String, String> specimenIds) {
    val outputRows = ImmutableList.<Tuple2<String, String>> builder();
    val key = getKey(row, dataType);

    val donorRetainFields = Lists.newArrayList(DONOR_FIELDS);
    val unwindFieldName = dataType.getId();
    donorRetainFields.add(unwindFieldName);

    val refinedDonor = row.deepCopy().retain(donorRetainFields);
    val parentFields = getDonorProjectIds(refinedDonor);

    val dataTypeUnwinder = Unwind.unwind(unwindFieldName);
    for (val nestedDataType : dataTypeUnwinder.call(refinedDonor)) {
      addParentValues(parentFields, nestedDataType);
      addSpecimenId(nestedDataType, specimenIds);

      val converter = new ConvertRow(dataType.getDownloadFields());
      val value = converter.call(nestedDataType);
      outputRows.add(tuple(key, value));
    }

    return outputRows.build();
  }

  private static void addSpecimenId(ObjectNode nestedDataType, Map<String, String> specimenIds) {
    val submittedSpecimenId = textValue(nestedDataType, SUBMISSION_SPECIMEN_ID);
    if (!isNullOrEmpty(submittedSpecimenId)) {
      val specimenId = specimenIds.get(submittedSpecimenId);
      if (!isNullOrEmpty(specimenId)) {
        nestedDataType.put(DONOR_SPECIMEN_ID, specimenId);
      }
    }
  }

  private static String resolveDonorNestedPath(DownloadDataType dataType) {
    val nestedName = dataType.getId();
    val donorName = DownloadDataType.DONOR.getId();

    return nestedName.replace(donorName + UNDERSCORE, EMPTY_STRING);
  }

  private static void addDonorStudyInvolvedIn(ObjectNode row, Multimap<String, String> study) {
    if (!study.isEmpty()) {
      val values = formatStudyValues(ImmutableSet.copyOf(study.values()));
      row.put(STUDY_DONOR_INVOLVED_IN, values);
    }
  }

  private static void addSpecimenStudyInvolvedIn(ObjectNode row, Multimap<String, String> study) {
    if (!study.isEmpty()) {
      val specimenId = getSpecimenId(row);
      val specimenStudy = ImmutableSet.copyOf(study.get(specimenId));

      if (!specimenStudy.isEmpty()) {
        val values = formatStudyValues(specimenStudy);
        row.put(STUDY_SPECIMEN_INVOLVED_IN, values);
      }
    }
  }

  private static void addParentValues(Map<String, String> donorValues, ObjectNode child) {
    donorValues.entrySet().stream()
        .forEach(entry -> child.put(entry.getKey(), entry.getValue()));
  }

  private static Map<String, String> getDonorProjectIds(ObjectNode donor) {
    return ImmutableMap.of(
        DONOR_ID, textValue(donor, DONOR_ID),
        SUBMISSION_DONOR_ID, textValue(donor, SUBMISSION_DONOR_ID),
        PROJECT_ID, textValue(donor, PROJECT_ID));
  }

  private static Map<String, String> getSpecimenIds(ObjectNode donor) {
    val values = ImmutableMap.<String, String> builder();
    values.putAll(getDonorProjectIds(donor));
    values.put(DONOR_SPECIMEN_ID, textValue(donor, DONOR_SPECIMEN_ID));
    values.put(SUBMISSION_SPECIMEN_ID, textValue(donor, SUBMISSION_SPECIMEN_ID));

    return values.build();
  }

  private static boolean isSpecimen(ObjectNode row, Optional<Multimap<String, String>> study) {
    return study.isPresent() && row.has(DONOR_SAMPLE);
  }

  /**
   * Creates a multimap of specimen_id to sample studies, where the samples belong to the specimen_id.<br>
   * This will be used in {@link org.icgc.dcc.release.job.export.record.RecordConverter} to resolve fields
   * {@code study_donor_involved_in} and {@code study_specimen_involved_in}.
   */
  private static Multimap<String, String> resolveStudy(ObjectNode row) {
    val studies = ArrayListMultimap.<String, String> create();
    for (val specimen : row.path(DONOR_SPECIMEN)) {
      val specimenId = getSpecimenId(specimen);
      for (val sample : specimen.path(DONOR_SAMPLE)) {
        val study = textValue(sample, DONOR_SAMPLE_STUDY);
        if (study != null) {
          studies.put(specimenId, study);
        }
      }
    }

    return studies;
  }

  private static String getKey(ObjectNode row, DownloadDataType dataType) {
    val donorId = textValue(row, DONOR_ID);
    checkState(!isNullOrEmpty(donorId), "Failed to resolve _donor_id from row: {}", row);
    return Keys.getKey(donorId, dataType.getId());
  }

  private static String formatStudyValues(Collection<String> studyValues) {
    return Joiners.COMMA.join(studyValues);
  }

  private static String getSpecimenId(JsonNode row) {
    return textValue(row, DONOR_SPECIMEN_ID);
  }

}
