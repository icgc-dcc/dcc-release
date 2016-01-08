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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.util.Collection;
import java.util.Map;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.icgc.dcc.release.core.model.Observation;
import org.icgc.dcc.release.core.util.Keys;
import org.icgc.dcc.release.job.join.model.DonorSample;
import org.icgc.dcc.release.job.join.model.SsmMetaFeatureType;
import org.icgc.dcc.release.job.join.model.SsmOccurrence;
import org.icgc.dcc.release.job.join.model.SsmOccurrence.Consequence;
import org.icgc.dcc.release.job.join.model.SsmOccurrenceFactory;
import org.icgc.dcc.release.job.join.model.SsmPrimaryFeatureType;
import org.icgc.dcc.release.job.join.utils.Occurrences;

import scala.Tuple2;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;

/**
 * Creates {@link SsmOccurrence}. At this point {@code SsmOccurrence.Observation} has only 1 {@link Observation} because
 * the SSM primary feature type was joined with the SSM secondary feature type.
 */
@RequiredArgsConstructor
public final class CreateOccurrence implements
    Function2<SsmOccurrence, Tuple2<SsmPrimaryFeatureType, Optional<Collection<Consequence>>>, SsmOccurrence> {

  /**
   * Dependencies.
   */
  @NonNull
  private final Broadcast<Map<String, SsmMetaFeatureType>> metaPairsBroadcast;
  @NonNull
  private final Map<String, DonorSample> donorSamples;
  @NonNull
  private final Map<String, String> sampleSurrogageSampleIds;

  @Override
  public SsmOccurrence call(SsmOccurrence aggregator,
      Tuple2<SsmPrimaryFeatureType, Optional<Collection<Consequence>>> tuple) throws Exception {
    // Get primary
    val primary = tuple._1;
    checkState(aggregator == null, "There should be only one instance of primary record: '%s'", primary);

    // Get meta
    val meta = getMeta(getPrimaryMetaKey(primary));
    checkNotNull(meta, "A primary record must have a corresponding meta record. %s", primary);

    // Create occurrence
    val occurrence = SsmOccurrenceFactory.createSsmOccurrence(primary, meta);

    // Enrich observation
    val observation = Occurrences.getObservation(occurrence);
    val matchedSampleId = meta.getMatched_sample_id();
    val analyzedSampleId = primary.getAnalyzed_sample_id();
    enrichObservation(analyzedSampleId, observation, matchedSampleId);

    // Enrich occurrence
    val surrogateDonorId = donorSamples.get(analyzedSampleId).getDonorId();
    occurrence.set_donor_id(surrogateDonorId);

    // Set consequences
    val consequencesOpt = tuple._2;
    if (consequencesOpt.isPresent()) {
      val consequences = consequencesOpt.get();
      occurrence.setConsequence(Sets.newHashSet(consequences));
    }

    return occurrence;
  }

  private void enrichObservation(String analyzedSampleId, Observation observation, String matchedSampleId) {
    val surrogateSpecimenId = donorSamples.get(analyzedSampleId).getSpecimenId();
    val surrogateSampleId = donorSamples.get(analyzedSampleId).getSampleId();
    observation.set_specimen_id(surrogateSpecimenId);
    observation.set_sample_id(surrogateSampleId);

    val surrogateMatchedSampleId = sampleSurrogageSampleIds.get(matchedSampleId);
    observation.set_matched_sample_id(surrogateMatchedSampleId);
  }

  private SsmMetaFeatureType getMeta(String key) {
    return metaPairsBroadcast.value().get(key);
  }

  private static String getPrimaryMetaKey(SsmPrimaryFeatureType primary) {
    return Keys.getKey(primary.getAnalysis_id(), primary.getAnalyzed_sample_id());
  }

}