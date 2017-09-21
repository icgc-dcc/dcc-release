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
package org.icgc.dcc.release.job.join.model;

import java.io.Serializable;

import lombok.Value;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;

/**
 * A model for a SGV Consequence. Used to decreases memory pressure.
 */
@Value
public class SgvConsequence implements Serializable {

  private static final Interner<String> STRING_INTERNER = Interners.newWeakInterner();

  @JsonProperty("observation_id")
  String observationId;
  @JsonProperty("consequence_type")
  String consequenceType;
  @JsonProperty("aa_change")
  String aaChange;
  @JsonProperty("cds_change")
  String cdsChange;
  @JsonProperty("protein_domain_affected")
  String proteinDomainAffected;
  @JsonProperty("gene_affected")
  String geneAffected;
  @JsonProperty("transcript_affected")
  String transcriptAffected;
  @JsonProperty("gene_build_version")
  String geneBuildVersion;
  String note;
  boolean coding;

  @JsonCreator
  public SgvConsequence(
      @JsonProperty("observation_id") String observationId,
      @JsonProperty("consequence_type") String consequenceType,
      @JsonProperty("aa_change") String aaChange,
      @JsonProperty("cds_change") String cdsChange,
      @JsonProperty("protein_domain_affected") String proteinDomainAffected,
      @JsonProperty("gene_affected") String geneAffected,
      @JsonProperty("transcript_affected") String transcriptAffected,
      @JsonProperty("gene_build_version") String geneBuildVersion,
      @JsonProperty("note") String note,
      @JsonProperty("coding") boolean coding)
  {
    this.observationId = intern(observationId);
    this.consequenceType = intern(consequenceType);
    this.aaChange = intern(aaChange);
    this.cdsChange = intern(cdsChange);
    this.proteinDomainAffected = intern(proteinDomainAffected);
    this.geneAffected = intern(geneAffected);
    this.transcriptAffected = intern(transcriptAffected);
    this.geneBuildVersion = intern(geneBuildVersion);
    this.note = intern(note);
    this.coding = coding;
  }

  private static String intern(String original) {
    return original == null ? null : STRING_INTERNER.intern(original);
  }

}
