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
package org.icgc.dcc.etl2.job.annotate.model;

import static lombok.AccessLevel.PRIVATE;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Header names of annotations produced by snpEff. Found in the INFO column of a VCF file.
 */
@RequiredArgsConstructor(access = PRIVATE)
@Getter
public enum InfoHeaderField {

  EFFECT_KEY(-1),
  CONSEQUENCE_TYPE_KEY(0),
  FUNCTIONAL_CLASS_KEY(1),
  CODON_CHANGE_KEY(2),
  AMINO_ACID_CHANGE_KEY(3),
  AMINO_ACID_LENGTH_KEY(4),
  GENE_NAME_KEY(5),
  GENE_BIOTYPE_KEY(6),
  CODING_KEY(7),
  TRANSCRIPT_ID_KEY(8),
  EXON_ID_KEY(9),
  CANCER_ID_KEY(10),
  PROTEIN_DOMAIN_KEY(-2);

  // Index within the effect metadata subfields from the SnpEff EFF annotation where each key's associated value can
  // be found during parsing.
  private final int fieldIndex;

}