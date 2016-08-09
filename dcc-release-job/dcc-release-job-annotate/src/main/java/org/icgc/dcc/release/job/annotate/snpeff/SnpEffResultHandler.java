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
package org.icgc.dcc.release.job.annotate.snpeff;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.regex.Pattern.compile;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.regex.Pattern;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;

import org.broadinstitute.variant.vcf.VCFCodec;
import org.broadinstitute.variant.vcf.VCFFormatHeaderLine;
import org.broadinstitute.variant.vcf.VCFHeader;
import org.broadinstitute.variant.vcf.VCFHeaderLine;
import org.broadinstitute.variant.vcf.VCFHeaderVersion;
import org.broadinstitute.variant.vcf.VCFInfoHeaderLine;
import org.icgc.dcc.release.job.annotate.converter.SnpEffVCFToICGCConverter;
import org.icgc.dcc.release.job.annotate.model.AnnotatedFileType;
import org.icgc.dcc.release.job.annotate.model.SecondaryEntity;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

public class SnpEffResultHandler implements Runnable {

  private static final Pattern SKIP_ANNOTATION_PATTERN = compile("^#|Reading cancer samples pedigree from VCF header");

  /**
   * Dependencies.
   */
  @NonNull
  private final InputStream input;
  private final VCFCodec decoder = createDecoder();

  /**
   * State.
   */
  @NonNull
  private final BlockingQueue<List<SecondaryEntity>> queue;
  @NonNull
  private final AnnotatedFileType fileType;
  private final SnpEffVCFToICGCConverter converter;

  public SnpEffResultHandler(@NonNull InputStream input, @NonNull BlockingQueue<List<SecondaryEntity>> queue,
      @NonNull AnnotatedFileType fileType, @NonNull String geneBuildVersion) {
    this.input = input;
    this.queue = queue;
    this.fileType = fileType;
    this.converter = new SnpEffVCFToICGCConverter(geneBuildVersion);
  }

  @Override
  @SneakyThrows
  public void run() {
    val reader = new BufferedReader(new InputStreamReader(input, UTF_8));
    String line = null;
    while ((line = reader.readLine()) != null) {
      if (isSkipLine(line)) {
        continue;
      }

      val variant = decoder.decode(line);
      val secondaryEntities = converter.convert(variant, fileType);
      queue.put(secondaryEntities);
    }

  }

  private static boolean isSkipLine(String line) {
    val matcher = SKIP_ANNOTATION_PATTERN.matcher(line);

    return matcher.find();
  }

  private VCFCodec createDecoder() {
    val decoder = new VCFCodec();
    val vcfHeader = createVCFHeader();
    decoder.setVCFHeader(vcfHeader, VCFHeaderVersion.VCF4_1);

    return decoder;
  }

  private static VCFHeader createVCFHeader() {
    Set<VCFHeaderLine> set = Sets.newHashSet();
    val line =
        new VCFFormatHeaderLine("<ID=GT,Number=1,Type=String,Description=\"Genotype\">", VCFHeaderVersion.VCF4_1);
    set.add(new VCFInfoHeaderLine(
        "<ID=EFF,Number=.,Type=String,Description=\"Predicted effects for this variant.Format: 'Effect ( Effect_Impact | Functional_Class | Codon_Change | Amino_Acid_Change| Amino_Acid_length | Gene_Name | Transcript_BioType | Gene_Coding | Transcript_ID | Exon_Rank  | Genotype_Number [ | ERRORS | WARNINGS ] )' \">",
        VCFHeaderVersion.VCF4_1));
    set.add(line);

    return new VCFHeader(set, ImmutableList.of("Patient_01_Germline", "Patient_01_Somatic"));
  }

  @SneakyThrows
  private PrintWriter openOutFile(String fileName) {
    return new PrintWriter(fileName, UTF_8.toString());
  }

}