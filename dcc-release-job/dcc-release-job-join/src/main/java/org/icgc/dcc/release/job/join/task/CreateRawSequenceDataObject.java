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
package org.icgc.dcc.release.job.join.task;

import static org.icgc.dcc.common.core.model.FieldNames.SEQUENCE_DATA_LIBRARY_STRATEGY;
import static org.icgc.dcc.common.core.model.FieldNames.SEQUENCE_DATA_REPOSITORY;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_ANALYZED_SAMPLE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_RAW_DATA_ACCESSION;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_RAW_DATA_REPOSITORY;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_SEQUENCING_STRATEGY;

import java.util.Map;

import lombok.val;

import org.apache.spark.api.java.function.Function;
import org.icgc.dcc.common.core.util.Jackson;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;

public final class CreateRawSequenceDataObject implements Function<ObjectNode, ObjectNode> {

  private static final Map<String, String> RAW_SEQUENCE_DATA_FIELDS_MAPPING = ImmutableMap.of(
      SUBMISSION_ANALYZED_SAMPLE_ID, SUBMISSION_ANALYZED_SAMPLE_ID,
      SUBMISSION_OBSERVATION_RAW_DATA_ACCESSION, SUBMISSION_OBSERVATION_RAW_DATA_ACCESSION,
      SUBMISSION_OBSERVATION_RAW_DATA_REPOSITORY, SEQUENCE_DATA_REPOSITORY,
      SUBMISSION_OBSERVATION_SEQUENCING_STRATEGY, SEQUENCE_DATA_LIBRARY_STRATEGY);

  @Override
  public ObjectNode call(ObjectNode node) throws Exception {
    val rawSequenceDataObject = Jackson.DEFAULT.createObjectNode();
    for (val entry : RAW_SEQUENCE_DATA_FIELDS_MAPPING.entrySet()) {
      rawSequenceDataObject.put(entry.getValue(), node.get(entry.getKey()));
    }

    return rawSequenceDataObject;
  }

}