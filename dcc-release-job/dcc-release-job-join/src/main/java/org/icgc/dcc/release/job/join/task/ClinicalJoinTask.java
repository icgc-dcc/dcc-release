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
package org.icgc.dcc.release.job.join.task;

import static org.icgc.dcc.release.core.job.FileType.BIOMARKER;
import static org.icgc.dcc.release.core.job.FileType.DONOR_SURROGATE_KEY;
import static org.icgc.dcc.release.core.job.FileType.EXPOSURE;
import static org.icgc.dcc.release.core.job.FileType.FAMILY;
import static org.icgc.dcc.release.core.job.FileType.SAMPLE_SURROGATE_KEY;
import static org.icgc.dcc.release.core.job.FileType.SPECIMEN_SURROGATE_KEY_IMAGE;
import static org.icgc.dcc.release.core.job.FileType.SURGERY;
import static org.icgc.dcc.release.core.job.FileType.THERAPY;
import static org.icgc.dcc.release.core.util.Partitions.getPartitionsCount;
import static org.icgc.dcc.release.core.util.Tasks.resolveProjectName;

import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.task.GenericTask;
import org.icgc.dcc.release.core.task.TaskContext;
import org.icgc.dcc.release.job.join.function.CombineClinical;
import org.icgc.dcc.release.job.join.function.CombineDonor;
import org.icgc.dcc.release.job.join.function.CombineSampleFunctions;
import org.icgc.dcc.release.job.join.function.CombineSpecimen;
import org.icgc.dcc.release.job.join.function.ExtractDonorId;
import org.icgc.dcc.release.job.join.function.ExtractSpecimenId;
import org.icgc.dcc.release.job.join.function.KeyDonorIdField;
import org.icgc.dcc.release.job.join.function.KeySpecimenIdField;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import scala.Tuple2;

@RequiredArgsConstructor
public class ClinicalJoinTask extends GenericTask {

  @NonNull
  private final Broadcast<Map<String, JavaRDD<ObjectNode>>> rawSequenceDataBroadcast;

  @Override
  public void execute(TaskContext taskContext) {
    val outputFileType = FileType.CLINICAL;

    val joinedSpecimen = joinSpecimen(taskContext);
    val joined = joinClinical(taskContext, joinedSpecimen);
    val output = joined.map(new CombineClinical());

    writeOutput(taskContext, output, outputFileType);
  }

  private JavaRDD<ObjectNode> joinSpecimen(TaskContext taskContext) {
    val specimen = readInput(taskContext, SPECIMEN_SURROGATE_KEY_IMAGE);
    val sample = readInput(taskContext, SAMPLE_SURROGATE_KEY);
    val joinedSample = joinSample(taskContext, sample);

    val joinedSpecimen = joinSpecimenSample(specimen, joinedSample);

    return joinedSpecimen.map(new CombineSpecimen());
  }

  private JavaPairRDD<String, Tuple2<ObjectNode, Optional<Iterable<ObjectNode>>>> joinClinical(
      TaskContext taskContext, JavaRDD<ObjectNode> joinedSpecimen) {
    val donor = joinDonor(taskContext);
    val donorPartitions = getPartitionsCount(donor);
    val pairedSpecimen = joinedSpecimen
        .mapToPair(new KeyDonorIdField())
        .groupByKey(donorPartitions);

    return donor.leftOuterJoin(pairedSpecimen);
  }

  private JavaPairRDD<String, ObjectNode> joinDonor(
      TaskContext taskContext) {
    val donor = readInput(taskContext, DONOR_SURROGATE_KEY);
    val therapy = readInput(taskContext, THERAPY);
    val family = readInput(taskContext, FAMILY);
    val exposure = readInput(taskContext, EXPOSURE);
    val biomarker = readInput(taskContext, BIOMARKER);
    val surgery = readInput(taskContext, SURGERY);
    val extractDonorId = new ExtractDonorId();
    val donorPairs = donor.mapToPair(new KeyDonorIdField());
    val donorPartitions = getPartitionsCount(donorPairs);

    val donorWithSupplementalFiles = donorPairs
        .leftOuterJoin(therapy.groupBy(extractDonorId, donorPartitions))
        .leftOuterJoin(family.groupBy(extractDonorId, donorPartitions))
        .leftOuterJoin(exposure.groupBy(extractDonorId, donorPartitions))
        .leftOuterJoin(biomarker.groupBy(extractDonorId, donorPartitions))
        .leftOuterJoin(surgery.groupBy(extractDonorId, donorPartitions));

    return donorWithSupplementalFiles.mapToPair(new CombineDonor());
  }

  private static JavaPairRDD<String, Tuple2<ObjectNode, Optional<Iterable<ObjectNode>>>> joinSpecimenSample(
      JavaRDD<ObjectNode> specimen, JavaRDD<ObjectNode> sample) {
    val extractSpecimenId = new ExtractSpecimenId();
    val specimenPairs = specimen.mapToPair(new KeySpecimenIdField());
    val specimenPartitions = getPartitionsCount(specimenPairs);

    return specimenPairs
        .leftOuterJoin(sample.groupBy(extractSpecimenId, specimenPartitions));
  }

  private JavaRDD<ObjectNode> joinSample(TaskContext taskContext, JavaRDD<ObjectNode> sample) {
    val rawSeqData = getRawSequenceData(taskContext);

    return sample
        .mapToPair(CombineSampleFunctions::pairSampleId)
        .leftOuterJoin(rawSeqData.groupBy(CombineSampleFunctions::extractSampleId))
        .map(CombineSampleFunctions::combineSample);
  }

  private JavaRDD<ObjectNode> getRawSequenceData(TaskContext taskContext) {
    val projectName = resolveProjectName(taskContext);

    return rawSequenceDataBroadcast.value().get(projectName);
  }

}
