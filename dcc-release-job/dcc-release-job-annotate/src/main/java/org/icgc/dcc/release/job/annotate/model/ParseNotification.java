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
package org.icgc.dcc.release.job.annotate.model;

import static lombok.AccessLevel.PRIVATE;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.core.model.Identifiable;

/**
 * Represents an error or warning produced by the SnpEff tool
 */
@Slf4j
@RequiredArgsConstructor(access = PRIVATE)
public enum ParseNotification implements Identifiable {
  UNKNOWN_NOTIFICATION,

  // 4.2 errors
  ERROR_CHROMOSOME_NOT_FOUND,
  ERROR_OUT_OF_CHROMOSOME_RANGE,

  WARNING_REF_DOES_NOT_MATCH_GENOME,
  WARNING_SEQUENCE_NOT_AVAILABLE,
  WARNING_TRANSCRIPT_INCOMPLETE,
  WARNING_TRANSCRIPT_MULTIPLE_STOP_COD,
  WARNING_TRANSCRIPT_NO_START_CODON,

  INFO_REALIGN_3_PRIME,
  INFO_COMPOUND_ANNOTATION,
  NFO_NON_REFERENCE_ANNOTATION,

  // 3.6
  PARSING_EXCEPTION,
  CDS_MUTATION_FAILURE,
  UNKNOWN_FUNCTIONAL_CLASS,
  MISSING_CONSEQUENCE_TYPE,
  WARNING_TRANSCRIPT_MULTIPLE_STOP_CODONS,
  ERROR_OUT_OF_EXON,
  ERROR_MISSING_CDS_SEQUENCE;

  public static final String SEPARATOR = "+";

  @Override
  public String getId() {
    return name();
  }

  public static ParseNotification getById(@NonNull String id) {
    for (val notification : ParseNotification.values()) {
      if (notification.getId().equals(id)) {
        return notification;
      }
    }
    log.debug("Unrecognized parse notificaiton '{}'", id);

    return UNKNOWN_NOTIFICATION;
  }

}
