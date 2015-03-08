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

import lombok.val;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.icgc.dcc.etl2.core.job.FileType;
import org.icgc.dcc.etl2.core.task.GenericTask;
import org.icgc.dcc.etl2.core.task.TaskContext;
import org.icgc.dcc.etl2.job.join.function.CombineClinical;
import org.icgc.dcc.etl2.job.join.function.ExtractSpecimenId;
import org.icgc.dcc.etl2.job.join.function.KeyDonorIdField;
import org.icgc.dcc.etl2.job.join.function.KeySpecimenIdField;
import org.icgc.dcc.etl2.job.join.function.PairDonorIdSpecimenSample;

import scala.Tuple2;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;

public class ClinicalJoinTask extends GenericTask {

  @Override
  public void execute(TaskContext taskContext) {
    val outputFileType = FileType.CLINICAL;

    taskContext.delete(outputFileType);

    val donor = parseDonor(taskContext);
    val specimen = parseSpecimen(taskContext);
    val sample = parseSample(taskContext);

    val joined = joinClinical(donor, specimen, sample);
    val output = joined.map(new CombineClinical());

    writeOutput(taskContext, output, outputFileType);
  }

  private JavaRDD<ObjectNode> parseDonor(TaskContext taskContext) {
    return readInput(taskContext, FileType.DONOR_SURROGATE_KEY);
  }

  private JavaRDD<ObjectNode> parseSpecimen(TaskContext taskContext) {
    return readInput(taskContext, FileType.SPECIMEN_SURROGATE_KEY_IMAGE);
  }

  private JavaRDD<ObjectNode> parseSample(TaskContext taskContext) {
    return readInput(taskContext, FileType.SAMPLE_SURROGATE_KEY);
  }

  private JavaPairRDD<String, Tuple2<ObjectNode, Optional<Iterable<Tuple2<ObjectNode, Optional<Iterable<ObjectNode>>>>>>> joinClinical(
      JavaRDD<ObjectNode> donor, JavaRDD<ObjectNode> specimen, JavaRDD<ObjectNode> sample) {
    val specimenSample = joinSpecimenSample(specimen, sample);

    val pairedDonor = donor.mapToPair(new KeyDonorIdField());
    val pairedSpecimenSample = specimenSample
        .mapToPair(new PairDonorIdSpecimenSample())
        .groupByKey();

    return pairedDonor.leftOuterJoin(pairedSpecimenSample);
  }

  private JavaPairRDD<String, Tuple2<ObjectNode, Optional<Iterable<ObjectNode>>>> joinSpecimenSample(
      JavaRDD<ObjectNode> specimen, JavaRDD<ObjectNode> sample) {
    return specimen
        .mapToPair(new KeySpecimenIdField())
        .leftOuterJoin(
            sample.groupBy(new ExtractSpecimenId()));
  }

}