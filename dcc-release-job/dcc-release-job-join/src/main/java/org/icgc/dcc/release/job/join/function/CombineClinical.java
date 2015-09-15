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
package org.icgc.dcc.release.job.join.function;

import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SPECIMEN;
import static org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames.PROJECT_ID;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_DONOR_ID;
import static org.icgc.dcc.release.core.util.FieldNames.JoinFieldNames.EXPOSURE;
import static org.icgc.dcc.release.core.util.FieldNames.JoinFieldNames.FAMILY;
import static org.icgc.dcc.release.core.util.FieldNames.JoinFieldNames.THERAPY;
import static org.icgc.dcc.release.job.join.utils.JsonNodes.populateArrayNode;
import lombok.val;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;

public class CombineClinical implements Function<Tuple2<String, Tuple2<Tuple2<Tuple2<Tuple2<ObjectNode,
    Optional<Iterable<ObjectNode>>>, Optional<Iterable<ObjectNode>>>, Optional<Iterable<ObjectNode>>>,
    Optional<Iterable<ObjectNode>>>>, ObjectNode> {

  @Override
  public ObjectNode call(Tuple2<String, Tuple2<Tuple2<Tuple2<Tuple2<ObjectNode, Optional<Iterable<ObjectNode>>>,
      Optional<Iterable<ObjectNode>>>, Optional<Iterable<ObjectNode>>>, Optional<Iterable<ObjectNode>>>> tuple)
      throws Exception {
    val donorTherapyTuple = tuple._2._1._1._1;
    val donor = donorTherapyTuple._1;

    if (donorTherapyTuple._2.isPresent()) {
      val therapy = donor.withArray(THERAPY);
      populateArrayNode(therapy, donorTherapyTuple._2.get(), CombineClinical::trimDuplicateFields);
    }

    val familyTuple = tuple._2._1._1;
    if (familyTuple._2.isPresent()) {
      val family = donor.withArray(FAMILY);
      populateArrayNode(family, familyTuple._2.get(), CombineClinical::trimDuplicateFields);
    }

    val exposureTuple = tuple._2._1;
    if (exposureTuple._2.isPresent()) {
      val exposure = donor.withArray(EXPOSURE);
      populateArrayNode(exposure, exposureTuple._2.get(), CombineClinical::trimDuplicateFields);
    }

    val specimenTuple = tuple._2;
    if (specimenTuple._2.isPresent()) {
      val specimen = donor.withArray(DONOR_SPECIMEN);
      populateArrayNode(specimen, specimenTuple._2.get(), CombineClinical::trimSpecimen);
    }

    return donor;
  }

  private static ObjectNode trimSpecimen(ObjectNode node) {
    node.remove(SUBMISSION_DONOR_ID);

    return node;
  }

  private static ObjectNode trimDuplicateFields(ObjectNode node) {
    node.remove(SUBMISSION_DONOR_ID);
    node.remove(PROJECT_ID);

    return node;
  }

}
