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

import java.util.List;
import java.util.regex.Pattern;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

/**
 * Represents an error or warning produced by the SnpEff tool
 */
@Slf4j
@Getter
@RequiredArgsConstructor(access = PRIVATE)
public enum ParseNotification {
  UNKNOWN_NOTIFICATION("UNKNOWN_NOTIFICATION"),
  PARSING_EXCEPTION("PARSING_EXCEPTION"),
  CDS_MUTATION_FAILURE("CDS_MUTATION_FAILURE"),
  UNKNOWN_FUNCTIONAL_CLASS("UNKNOWN_FUNCTIONAL_CLASS"),
  MISSING_CONSEQUENCE_TYPE("MISSING_CONSEQUENCE_TYPE"),
  WARNING_SEQUENCE_NOT_AVAILABLE("WARNING_SEQUENCE_NOT_AVAILABLE"),
  WARNING_REF_DOES_NOT_MATCH_GENOME("WARNING_REF_DOES_NOT_MATCH_GENOME"),
  WARNING_TRANSCRIPT_INCOMPLETE("WARNING_TRANSCRIPT_INCOMPLETE"),
  WARNING_TRANSCRIPT_MULTIPLE_STOP_CODONS("WARNING_TRANSCRIPT_MULTIPLE_STOP_CODONS"),
  WARNING_TRANSCRIPT_NO_START_CODON("WARNING_TRANSCRIPT_NO_START_CODON"),
  ERROR_CHROMOSOME_NOT_FOUND("ERROR_CHROMOSOME_NOT_FOUND"),
  ERROR_OUT_OF_CHROMOSOME_RANGE("ERROR_OUT_OF_CHROMOSOME_RANGE"),
  ERROR_OUT_OF_EXON("ERROR_OUT_OF_EXON"),
  ERROR_MISSING_CDS_SEQUENCE("ERROR_MISSING_CDS_SEQUENCE");

  public static final String SEPARATOR = "+";
  private static final Pattern MULTI_CONSEQUENCE_PATTERN = Pattern.compile("(\\w+)\\+(\\w+)");
  private static final Splitter SPLITTER = Splitter.on('+');

  @NonNull
  private final String id;

  public static ParseNotification getById(@NonNull String id) {
    for (val notification : ParseNotification.values()) {
      if (notification.getId().equals(id)) {
        return notification;
      }
    }
    log.warn("Unrecognized parse notificaiton '{}'", id);

    return UNKNOWN_NOTIFICATION;
  }

  public static List<ParseNotification> parse(@NonNull String annotation) {
    val result = new ImmutableList.Builder<ParseNotification>();
    val matcher = MULTI_CONSEQUENCE_PATTERN.matcher(annotation);

    if (matcher.find()) {
      for (val notificationId : SPLITTER.split(annotation)) {
        result.add(getById(notificationId));
      }
    } else {
      result.add(getById(annotation));
    }

    return result.build();
  }

}
