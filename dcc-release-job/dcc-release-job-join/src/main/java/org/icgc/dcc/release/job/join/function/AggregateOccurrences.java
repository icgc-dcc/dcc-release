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

import java.util.Set;

import lombok.val;

import org.apache.spark.api.java.function.Function2;
import org.icgc.dcc.release.job.join.model.SsmOccurrence;
import org.icgc.dcc.release.job.join.model.SsmOccurrence.Consequence;

import com.google.common.collect.Sets;

public class AggregateOccurrences implements Function2<SsmOccurrence, SsmOccurrence, SsmOccurrence> {

  @Override
  public SsmOccurrence call(SsmOccurrence aggregator, SsmOccurrence occurrence) throws Exception {
    if (aggregator == null) {
      return occurrence;
    }

    val occurrenceConsequences = occurrence.getConsequence();
    if (occurrenceConsequences != null) {
      getConsequeces(aggregator).addAll(occurrenceConsequences);
    }

    aggregator.getObservation().addAll(occurrence.getObservation());

    return aggregator;
  }

  private static Set<Consequence> getConsequeces(SsmOccurrence occurrence) {
    Set<Consequence> consequences = occurrence.getConsequence();
    if (consequences == null) {
      consequences = Sets.newHashSet();
      occurrence.setConsequence(consequences);
    }

    return consequences;
  }

}
