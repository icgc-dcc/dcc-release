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
package org.icgc.dcc.release.job.annotate.model;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

/**
 * A snpEff 3.6 representation.
 */
@Value
@Builder
public final class SnpEffect implements Comparable<SnpEffect> {

  ConsequenceType consequenceType;
  String codonChange;
  String aminoAcidChange;
  String aminoAcidPosition;
  String geneID;
  String transcriptID;
  String cancerID;
  String allele;
  ParseState parseState;

  @Override
  public int compareTo(SnpEffect otherEffect) {
    return consequenceType.compareTo(otherEffect.getConsequenceType());
  }

  /**
   * Checks if there are annotation parsing errors or warnings.
   */
  public boolean hasError() {
    return parseState != null ? parseState.hasError() : false;
  }

  public boolean containsAnyError(@NonNull ParseNotification... error) {
    return parseState.containsAnyError(error);
  }

}