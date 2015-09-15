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
package org.icgc.dcc.release.job.orphan.task;

import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.size;
import static com.google.common.collect.Maps.filterKeys;
import static com.google.common.collect.Maps.filterValues;
import static com.google.common.collect.Sets.filter;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collector;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.JavaRDD;
import org.icgc.dcc.common.core.model.FieldNames;
import org.icgc.dcc.release.core.function.CombineFields;
import org.icgc.dcc.release.core.function.ExtractFieldPair;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.task.TaskContext;
import org.icgc.dcc.release.core.util.ObjectNodeRDDs;
import org.icgc.dcc.release.job.orphan.model.Orphans;

import scala.Tuple2;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

@Slf4j
@RequiredArgsConstructor
public class OrphanResolver {

  @NonNull
  private final TaskContext taskContext;

  public Orphans resolveOrphans() {
    val donorIds = readDonorIds();
    val specimenDonorIds = readSpecimenDonorIds();
    val sampleSpecimenIds = readSampleSpecimenIds();
    val metaSampleIds = readMetaSampleIds();

    val validDonorIds = calculateValidDonorIds(specimenDonorIds, sampleSpecimenIds, metaSampleIds);

    val orphanedDonorIds = calculateOrphanDonorIds(donorIds, validDonorIds);
    val orphanedSpecimenIds = calcuateOrphanedSpecimenIds(specimenDonorIds, orphanedDonorIds);
    val orphanedSampleIds = calculateOrphanedSampleIds(sampleSpecimenIds, orphanedSpecimenIds);

    return new Orphans(orphanedDonorIds, orphanedSpecimenIds, orphanedSampleIds);
  }

  private Set<String> calculateOrphanedSampleIds(Map<String, String> sampleSpecimenIds, Set<String> orphanedSpecimenIds) {
    val orphanedSampleIds =
        ImmutableMap.copyOf(filterValues(sampleSpecimenIds, in(orphanedSpecimenIds))).keySet();

    log.info("Total Samples: {}, total valid: {}, total orphans: {}",
        size(sampleSpecimenIds.keySet()),
        size(sampleSpecimenIds.keySet()) - size(orphanedSampleIds),
        size(orphanedSampleIds));

    return orphanedSampleIds;
  }

  private Set<String> calcuateOrphanedSpecimenIds(Map<String, String> specimenDonorIds, Set<String> orphanedDonorIds) {
    val orphanedSpecimenIds =
        ImmutableMap.copyOf(filterValues(specimenDonorIds, in(orphanedDonorIds))).keySet();

    log.info("Total Specimen: {}, total valid: {}, total orphans: {}",
        size(specimenDonorIds.keySet()),
        size(specimenDonorIds.keySet()) - size(orphanedSpecimenIds),
        size(orphanedSpecimenIds));

    return orphanedSpecimenIds;
  }

  private Set<String> calculateOrphanDonorIds(Set<String> donorIds, Set<String> validDonorIds) {
    val orphanedDonorIds = ImmutableSet.copyOf(filter(donorIds, not(in(validDonorIds))));

    log.info("Total donors: {}, total valid: {}, total orphans: {}", size(donorIds),
        size(validDonorIds), Iterables.size(orphanedDonorIds));

    return orphanedDonorIds;
  }

  private Set<String> calculateValidDonorIds(Map<String, String> specimenDonorIds,
      Map<String, String> sampleSpecimenIds, Set<String> metaSampleIds) {
    log.info("Finding valid ids");
    val validSampleIdToSpecimenIdMappings =
        ImmutableMap.copyOf(filterKeys(sampleSpecimenIds, in(metaSampleIds)));

    val validSpecimenIds = Sets.<String> newHashSet(validSampleIdToSpecimenIdMappings.values());

    val validSpecimenIdToDonorIdMappings =
        ImmutableMap.copyOf(filterKeys(specimenDonorIds, in(validSpecimenIds)));
    val validDonorIdsSet = Sets.<String> newHashSet(validSpecimenIdToDonorIdMappings.values());
    log.info("Finished finding valid ids");

    return validDonorIdsSet;
  }

  private Set<String> readDonorIds() {
    val donorType = FileType.DONOR;

    if (isMissing(donorType)) {
      return emptySet();
    }

    return readSet(donorType, FieldNames.SubmissionFieldNames.SUBMISSION_DONOR_ID);
  }

  private Map<String, String> readSpecimenDonorIds() {
    return readMap(
        FileType.SPECIMEN,
        FieldNames.SubmissionFieldNames.SUBMISSION_SPECIMEN_ID,
        FieldNames.SubmissionFieldNames.SUBMISSION_DONOR_ID);
  }

  private Map<String, String> readSampleSpecimenIds() {
    return readMap(
        FileType.SAMPLE,
        FieldNames.SubmissionFieldNames.SUBMISSION_ANALYZED_SAMPLE_ID,
        FieldNames.SubmissionFieldNames.SUBMISSION_SPECIMEN_ID);
  }

  private Set<String> readMetaSampleIds() {
    val metaSampleIds = Sets.<String> newHashSet();
    for (val fileType : FileType.values()) {
      if (!fileType.isMetaFileType()) {
        continue;
      }

      if (isMissing(fileType)) {
        continue;
      }

      metaSampleIds.addAll(
          readSet(fileType, FieldNames.SubmissionFieldNames.SUBMISSION_ANALYZED_SAMPLE_ID));
    }

    return metaSampleIds;
  }

  private Set<String> readSet(FileType fileType, String fieldName) {
    return readFileType(fileType)
        .map(new CombineFields(fieldName))
        .distinct()
        .collect()
        .stream()
        .collect(toSet());
  }

  private Map<String, String> readMap(FileType fileType, String keyFieldName, String valueFieldName) {
    if (isMissing(fileType)) {
      return emptyMap();
    }

    return readFileType(fileType)
        .map(new ExtractFieldPair(keyFieldName, valueFieldName))
        .distinct()
        .collect()
        .stream()
        .collect(toTupleMap());
  }

  private JavaRDD<ObjectNode> readFileType(FileType fileType) {
    val metaFileTypePath = taskContext.getPath(fileType);

    return ObjectNodeRDDs.textObjectNodeFile(taskContext.getSparkContext(), metaFileTypePath);
  }

  private boolean isMissing(FileType fileType) {
    return !taskContext.exists(fileType);
  }

  private static Collector<Tuple2<String, String>, ?, Map<String, String>> toTupleMap() {
    return toMap(Tuple2<String, String>::_1, Tuple2<String, String>::_2);
  }

}