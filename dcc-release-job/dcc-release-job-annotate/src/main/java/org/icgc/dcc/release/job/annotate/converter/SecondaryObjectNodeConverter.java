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
package org.icgc.dcc.release.job.annotate.converter;

import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.release.core.model.CodingTypes.fieldNameForCoding;
import static org.icgc.dcc.release.core.model.ConsequenceType.*;
import static org.icgc.dcc.release.core.model.ConsequenceType.STOP_RETAINED_VARIANT;

import com.google.common.collect.ImmutableSet;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import org.icgc.dcc.common.core.model.FieldNames;
import org.icgc.dcc.release.core.model.CodingTypes;
import org.icgc.dcc.release.job.annotate.model.AnnotatedFileType;
import org.icgc.dcc.release.job.annotate.model.SecondaryEntity;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Set;

@Slf4j
@NoArgsConstructor(access = PRIVATE)
public final class SecondaryObjectNodeConverter {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static ObjectNode convert(SecondaryEntity secondaryEntity, AnnotatedFileType fileType) {

    val secondary = MAPPER.createObjectNode();
    val aaMutation = secondaryEntity.getAaMutation();
    val cdsMutation = secondaryEntity.getCdsMutation();
    if (fileType == AnnotatedFileType.SSM) {
      secondary.put(FieldNames.CONSEQUENCE_AA_MUTATION, aaMutation);
      secondary.put(FieldNames.AnnotatorFieldNames.ANNOTATOR_CDS_MUTATION, cdsMutation);
    } else {
      secondary.put(FieldNames.AnnotatorFieldNames.ANNOTATOR_AMINO_ACID_CHANGE, aaMutation);
      secondary.put(FieldNames.AnnotatorFieldNames.ANNOTATOR_CDS_CHANGE, cdsMutation);
    }

    secondary.put(FieldNames.MUTATION_CONSEQUENCE_TYPES, secondaryEntity.getConsequenceType());
    secondary.put(FieldNames.AnnotatorFieldNames.ANNOTATOR_PROTEIN_DOMAIN_AFFECTED,
        secondaryEntity.getProteinDomainAffected());
    secondary.put(FieldNames.SubmissionFieldNames.SUBMISSION_GENE_AFFECTED, secondaryEntity.getGeneAffected());
    secondary.put(FieldNames.SubmissionFieldNames.SUBMISSION_TRANSCRIPT_AFFECTED,
        secondaryEntity.getTranscriptAffected());
    secondary.put(FieldNames.AnnotatorFieldNames.ANNOTATOR_GENE_BUILD_VERSION, secondaryEntity.getGeneBuildVersion());
    secondary.put(FieldNames.AnnotatorFieldNames.ANNOTATOR_NOTE, secondaryEntity.getNote());
    secondary.put(FieldNames.NormalizerFieldNames.NORMALIZER_OBSERVATION_ID, secondaryEntity.getObservationId());
    
    return secondary;
  }

}
