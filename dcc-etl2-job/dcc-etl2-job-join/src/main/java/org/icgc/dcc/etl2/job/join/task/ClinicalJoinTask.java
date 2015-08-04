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
package org.icgc.dcc.etl2.job.join.task;

import static org.icgc.dcc.etl2.core.job.FileType.BIOMARKER;
import static org.icgc.dcc.etl2.core.job.FileType.DONOR_SURROGATE_KEY;
import static org.icgc.dcc.etl2.core.job.FileType.EXPOSURE;
import static org.icgc.dcc.etl2.core.job.FileType.FAMILY;
import static org.icgc.dcc.etl2.core.job.FileType.SAMPLE_SURROGATE_KEY;
import static org.icgc.dcc.etl2.core.job.FileType.SPECIMEN_SURROGATE_KEY_IMAGE;
import static org.icgc.dcc.etl2.core.job.FileType.SURGERY;
import static org.icgc.dcc.etl2.core.job.FileType.THERAPY;
import lombok.val;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.icgc.dcc.etl2.core.job.FileType;
import org.icgc.dcc.etl2.core.task.GenericTask;
import org.icgc.dcc.etl2.core.task.TaskContext;
import org.icgc.dcc.etl2.job.join.function.CombineClinical;
import org.icgc.dcc.etl2.job.join.function.CombineSpecimen;
import org.icgc.dcc.etl2.job.join.function.ExtractDonorId;
import org.icgc.dcc.etl2.job.join.function.ExtractSpecimenId;
import org.icgc.dcc.etl2.job.join.function.KeyDonorIdField;
import org.icgc.dcc.etl2.job.join.function.KeySpecimenIdField;

import scala.Tuple2;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;

public class ClinicalJoinTask extends GenericTask {

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
    val biomarker = readInput(taskContext, BIOMARKER);
    val surgery = readInput(taskContext, SURGERY);

    val joinedSpecimen = joinSpecimenSample(specimen, sample, biomarker, surgery);

    return joinedSpecimen.map(new CombineSpecimen());
  }

  private JavaPairRDD<String, Tuple2<Tuple2<Tuple2<Tuple2<ObjectNode, Optional<Iterable<ObjectNode>>>,
      Optional<Iterable<ObjectNode>>>, Optional<Iterable<ObjectNode>>>, Optional<Iterable<ObjectNode>>>> joinClinical(
          TaskContext taskContext, JavaRDD<ObjectNode> joinedSpecimen) {
    val donor = joinDonor(taskContext);
    val pairedSpecimen = joinedSpecimen
        .mapToPair(new KeyDonorIdField())
        .groupByKey();

    return donor.leftOuterJoin(pairedSpecimen);
  }

  private JavaPairRDD<String, Tuple2<Tuple2<Tuple2<ObjectNode, Optional<Iterable<ObjectNode>>>,
      Optional<Iterable<ObjectNode>>>, Optional<Iterable<ObjectNode>>>> joinDonor(TaskContext taskContext) {
    val donor = readInput(taskContext, DONOR_SURROGATE_KEY);
    val therapy = readInput(taskContext, THERAPY);
    val family = readInput(taskContext, FAMILY);
    val exposure = readInput(taskContext, EXPOSURE);
    val extractDonorId = new ExtractDonorId();

    return donor
        .mapToPair(new KeyDonorIdField())
        .leftOuterJoin(therapy.groupBy(extractDonorId))
        .leftOuterJoin(family.groupBy(extractDonorId))
        .leftOuterJoin(exposure.groupBy(extractDonorId));
  }

  private static JavaPairRDD<String, Tuple2<Tuple2<Tuple2<ObjectNode, Optional<Iterable<ObjectNode>>>, Optional<Iterable<ObjectNode>>>,
      Optional<Iterable<ObjectNode>>>> joinSpecimenSample(JavaRDD<ObjectNode> specimen, JavaRDD<ObjectNode> sample,
          JavaRDD<ObjectNode> biomarker, JavaRDD<ObjectNode> therapy) {
    val extractSpecimenId = new ExtractSpecimenId();

    return specimen
        .mapToPair(new KeySpecimenIdField())
        .leftOuterJoin(sample.groupBy(extractSpecimenId))
        .leftOuterJoin(biomarker.groupBy(extractSpecimenId))
        .leftOuterJoin(therapy.groupBy(extractSpecimenId));
  }

}