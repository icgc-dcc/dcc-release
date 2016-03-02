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
package org.icgc.dcc.release.test.function;

import static org.icgc.dcc.common.core.model.FieldNames.SEQUENCE_DATA_LIBRARY_STRATEGY;
import static org.icgc.dcc.common.core.model.FieldNames.SEQUENCE_DATA_REPOSITORY;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_RAW_DATA_ACCESSION;
import static org.icgc.dcc.common.json.Jackson.asArrayNode;
import static org.icgc.dcc.common.json.Jackson.asObjectNode;

import java.util.Collections;

import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.common.core.model.FieldNames;
import org.icgc.dcc.common.json.Jackson;
import org.icgc.dcc.release.core.util.ArrayNodes;
import org.icgc.dcc.release.core.util.Keys;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;

public final class DonorJsonComparator extends JsonComparator {

  private static final String[] SEQUENCING_DATA_KEYS = { SUBMISSION_OBSERVATION_RAW_DATA_ACCESSION,
      SEQUENCE_DATA_REPOSITORY, SEQUENCE_DATA_LIBRARY_STRATEGY };

  @Override
  protected void compare(@NonNull ObjectNode actual, @NonNull ObjectNode expected) {
    super.compare(normalizeDonor(actual), expected);
  }

  private static ObjectNode normalizeDonor(ObjectNode donor) {
    normalizeSpecimen(donor);
    normalizeSummary(donor);

    return donor;
  }

  private static void normalizeSpecimen(ObjectNode donor) {
    val speciments = donor.path(FieldNames.DONOR_SPECIMEN);
    if (speciments.isMissingNode()) {
      return;
    }

    for (val specimen : speciments) {
      normalizeSample(specimen);

    }

  }

  private static void normalizeSample(JsonNode specimen) {
    val samples = specimen.path(FieldNames.DONOR_SAMPLE);
    if (samples.isMissingNode()) {
      return;
    }

    for (val sample : samples) {
      normalizeAvailableRawSequenceData(sample);
    }
  }

  private static void normalizeAvailableRawSequenceData(JsonNode sample) {
    val sequencingDataNode = sample.path(FieldNames.DONOR_SAMPLE_SEQUENCE_DATA);

    if (sequencingDataNode.isMissingNode()) {
      return;
    }

    val sequencingData = ArrayNodes.toMutableList(asArrayNode(sequencingDataNode));
    Collections.sort(sequencingData, DonorJsonComparator::compareSequencingData);
    val sampleObject = Jackson.asObjectNode(sample);
    sampleObject.set(FieldNames.DONOR_SAMPLE_SEQUENCE_DATA, ArrayNodes.toArrayNode(sequencingData));
  }

  private static int compareSequencingData(JsonNode left, JsonNode right) {
    val leftKey = Keys.getKey(asObjectNode(left), SEQUENCING_DATA_KEYS);
    val rightKey = Keys.getKey(asObjectNode(right), SEQUENCING_DATA_KEYS);

    return leftKey.compareTo(rightKey);
  }

  private static void normalizeSummary(ObjectNode donor) {
    val summaryNode = donor.path(FieldNames.DONOR_SUMMARY);
    if (summaryNode.isMissingNode()) {
      return;
    }

    val summary = asObjectNode(summaryNode);
    val dataTypes = asArrayNode(summary.get(FieldNames.AVAILABLE_DATA_TYPES));
    val sortedDataTypes = sortStringsArrayNode(dataTypes);
    summary.set(FieldNames.AVAILABLE_DATA_TYPES, sortedDataTypes);
  }

  private static ArrayNode sortStringsArrayNode(ArrayNode arrayNode) {
    val list = Lists.<String> newArrayList();
    for (val element : arrayNode) {
      list.add(element.textValue());
    }

    Collections.sort(list);

    val sortedArrayNode = arrayNode.arrayNode();
    list.forEach(e -> sortedArrayNode.add(e));

    return sortedArrayNode;
  }

}