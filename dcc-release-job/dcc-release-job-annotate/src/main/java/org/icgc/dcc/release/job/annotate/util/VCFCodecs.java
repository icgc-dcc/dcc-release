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
package org.icgc.dcc.release.job.annotate.util;

import static lombok.AccessLevel.PRIVATE;
import lombok.NoArgsConstructor;
import lombok.val;

import org.broadinstitute.variant.vcf.VCFCodec;
import org.broadinstitute.variant.vcf.VCFFormatHeaderLine;
import org.broadinstitute.variant.vcf.VCFHeader;
import org.broadinstitute.variant.vcf.VCFHeaderVersion;
import org.broadinstitute.variant.vcf.VCFInfoHeaderLine;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

@NoArgsConstructor(access = PRIVATE)
public final class VCFCodecs {

  public static VCFCodec createDecoder() {
    val decoder = new VCFCodec();
    val vcfHeader = createVCFHeader();
    decoder.setVCFHeader(vcfHeader, VCFHeaderVersion.VCF4_1);

    return decoder;
  }

  private static VCFHeader createVCFHeader() {
    return new VCFHeader(
        ImmutableSet.of(
            new VCFInfoHeaderLine(
                "<ID=EFF,Number=.,Type=String,Description=\"Predicted effects for this variant.Format: 'Effect ( Effect_Impact | Functional_Class | Codon_Change | Amino_Acid_Change| Amino_Acid_length | Gene_Name | Transcript_BioType | Gene_Coding | Transcript_ID | Exon_Rank  | Genotype_Number [ | ERRORS | WARNINGS ] )' \">",
                VCFHeaderVersion.VCF4_1),
            new VCFFormatHeaderLine(
                "<ID=GT,Number=1,Type=String,Description=\"Genotype\">",
                VCFHeaderVersion.VCF4_1)),
        ImmutableList.of(
            "Patient_01_Germline", "Patient_01_Somatic"));
  }

}
