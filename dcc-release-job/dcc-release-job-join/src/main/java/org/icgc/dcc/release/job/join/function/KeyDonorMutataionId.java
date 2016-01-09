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

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.release.core.util.Keys.getKey;
import static org.icgc.dcc.release.core.util.Tuples.tuple;

import java.util.Map;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.spark.api.java.function.PairFunction;
import org.icgc.dcc.release.job.join.model.DonorSample;
import org.icgc.dcc.release.job.join.model.SsmOccurrence;
import org.icgc.dcc.release.job.join.utils.Occurrences;

import scala.Tuple2;

@RequiredArgsConstructor
public final class KeyDonorMutataionId implements PairFunction<Tuple2<String, SsmOccurrence>, String, SsmOccurrence> {

  /**
   * Used to resolve {@code _donor_id}.
   */
  @NonNull
  private final Map<String, DonorSample> donorSamples;

  @Override
  public Tuple2<String, SsmOccurrence> call(Tuple2<String, SsmOccurrence> tuple) throws Exception {
    val primary = tuple._2;
    val mutationId = primary.get_mutation_id();
    val sampleId = resolveSampleId(primary);

    val donorInfo = donorSamples.get(sampleId);
    checkState(donorInfo != null, "Failed to resolve donor info for sample id '%s' from ssm_p: '%s'", sampleId, primary);
    val key = getKey(donorInfo.getDonorId(), mutationId);

    return tuple(key, primary);
  }

  private static String resolveSampleId(SsmOccurrence occurrence) {
    val observation = Occurrences.getObservation(occurrence);

    return observation.getAnalyzed_sample_id();
  }

}