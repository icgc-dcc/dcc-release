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
package org.icgc.dcc.release.job.join.function;

import static org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames.PROJECT_ID;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_DONOR_ID;
import static org.icgc.dcc.release.core.util.FieldNames.JoinFieldNames.BIOMARKER;
import static org.icgc.dcc.release.core.util.FieldNames.JoinFieldNames.EXPOSURE;
import static org.icgc.dcc.release.core.util.FieldNames.JoinFieldNames.FAMILY;
import static org.icgc.dcc.release.core.util.FieldNames.JoinFieldNames.SURGERY;
import static org.icgc.dcc.release.core.util.FieldNames.JoinFieldNames.THERAPY;
import static org.icgc.dcc.release.core.util.Tuples.tuple;
import static org.icgc.dcc.release.job.join.utils.JsonNodes.populateArrayNode;

import org.apache.spark.api.java.function.PairFunction;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;

import lombok.val;
import scala.Tuple2;

/**
 * Combines donor with its supplemental file types.
 */
public class CombineDonor implements
    PairFunction<Tuple2<String, Tuple2<Tuple2<Tuple2<Tuple2<Tuple2<ObjectNode, Optional<Iterable<ObjectNode>>>, Optional<Iterable<ObjectNode>>>, Optional<Iterable<ObjectNode>>>, Optional<Iterable<ObjectNode>>>, Optional<Iterable<ObjectNode>>>>, String, ObjectNode> {

  @Override
  public Tuple2<String, ObjectNode> call(
      Tuple2<String, Tuple2<Tuple2<Tuple2<Tuple2<Tuple2<ObjectNode, Optional<Iterable<ObjectNode>>>, Optional<Iterable<ObjectNode>>>, Optional<Iterable<ObjectNode>>>, Optional<Iterable<ObjectNode>>>, Optional<Iterable<ObjectNode>>>> tuple)
      throws Exception {
    val donorId = tuple._1;
    val donorSupplemental = tuple._2;
    val donorButSurgery = donorSupplemental._1;
    val surgeryOpt = donorSupplemental._2;
    val biomarkerOpt = donorButSurgery._2;
    val donorButBiomarker = donorButSurgery._1;
    val exposureOpt = donorButBiomarker._2;
    val donorButExposure = donorButBiomarker._1;
    val familyOpt = donorButExposure._2;
    val donorButFamily = donorButExposure._1;
    val therapyOpt = donorButFamily._2;
    val donor = donorButFamily._1;

    if (therapyOpt.isPresent()) {
      val therapy = donor.withArray(THERAPY);
      populateArrayNode(therapy, therapyOpt.get(), CombineDonor::trimDuplicateFields);
    }

    if (familyOpt.isPresent()) {
      val family = donor.withArray(FAMILY);
      populateArrayNode(family, familyOpt.get(), CombineDonor::trimDuplicateFields);
    }

    if (exposureOpt.isPresent()) {
      val exposure = donor.withArray(EXPOSURE);
      populateArrayNode(exposure, exposureOpt.get(), CombineDonor::trimDuplicateFields);
    }

    if (biomarkerOpt.isPresent()) {
      val biomarker = donor.withArray(BIOMARKER);
      populateArrayNode(biomarker, biomarkerOpt.get(), CombineDonor::trimDuplicateFields);
    }

    if (surgeryOpt.isPresent()) {
      val surgery = donor.withArray(SURGERY);
      populateArrayNode(surgery, surgeryOpt.get(), CombineDonor::trimDuplicateFields);
    }

    return tuple(donorId, donor);
  }

  private static ObjectNode trimDuplicateFields(ObjectNode node) {
    node.remove(SUBMISSION_DONOR_ID);
    node.remove(PROJECT_ID);

    return node;
  }

}