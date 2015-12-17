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

import static org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames.OBSERVATION_ARRAY_NAME;
import static org.icgc.dcc.release.job.join.utils.CombineFunctions.mergeConsequences;
import lombok.val;

import org.apache.spark.api.java.function.Function2;

import com.fasterxml.jackson.databind.node.ObjectNode;

public final class CombineObservations implements Function2<ObjectNode, ObjectNode, ObjectNode> {

  @Override
  public ObjectNode call(ObjectNode leftOccurrence, ObjectNode rightOccurrence) throws Exception {
    mergeObservations(leftOccurrence, rightOccurrence);
    mergeConsequences(leftOccurrence, rightOccurrence);

    return leftOccurrence;
  }

  private static void mergeObservations(ObjectNode leftOccurrence, ObjectNode rightOccurrence) {
    val leftObservations = leftOccurrence.withArray(OBSERVATION_ARRAY_NAME);
    val rightObservations = rightOccurrence.withArray(OBSERVATION_ARRAY_NAME);
    leftObservations.addAll(rightObservations);
  }

}