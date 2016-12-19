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
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Header names of annotations produced by snpEff. Found in the INFO column of a VCF file.
 */
@Getter
@RequiredArgsConstructor(access = PRIVATE)
public enum InfoHeaderField {

  ALLELE(0),
  CONSEQUENCE_TYPE_INDEX(1),
  GENE_ID_INDEX(4),
  FEATURE_ID_INDEX(6),
  HGVS_C_INDEX(9),
  HGVS_P_INDEX(10),
  CDNA_POSITION_INDEX(11),
  ERRORS_INDEX(15);

  // Index within the effect metadata subfields from the SnpEff EFF annotation where each key's associated value can
  // be found during parsing.
  private final int fieldIndex;

}