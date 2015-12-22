/*
 * Copyright (c) 2014 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.release.job.document.transform;

import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_SUMMARY;
import static org.icgc.dcc.common.core.model.FieldNames.MUTATION_SUMMARY_AFFECTED_DONOR_COUNT;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getDonorId;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getMutationObservationDonor;
import static org.icgc.dcc.release.job.document.model.CollectionFieldAccessors.getMutationOccurrences;

import java.util.TreeSet;

import lombok.val;

import org.icgc.dcc.release.core.document.Document;
import org.icgc.dcc.release.job.document.core.DocumentCallback;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Sets;

public class MutationCentricSummaryCallback implements DocumentCallback {

  @Override
  public void call(Document document) {
    // Mutation with observations
    ObjectNode mutation = document.getSource();
    val mutationOccurrences = getMutationOccurrences(mutation);

    // Distinct
    val donorIds = newTreeSet();

    for (val mutationOccurrence : mutationOccurrences) {
      // Collect donor id
      val donor = getMutationObservationDonor((ObjectNode) mutationOccurrence);
      val donorId = getDonorId(donor);
      donorIds.add(donorId);
    }

    /**
     * Summary: {@code mutation._summary}.
     */

    // Create summary
    val mutationSummary = mutation.objectNode();
    mutationSummary.put(MUTATION_SUMMARY_AFFECTED_DONOR_COUNT, donorIds.size());

    mutation.set(MUTATION_SUMMARY, mutationSummary);
  }

  private static TreeSet<String> newTreeSet() {
    return Sets.<String> newTreeSet();
  }

}