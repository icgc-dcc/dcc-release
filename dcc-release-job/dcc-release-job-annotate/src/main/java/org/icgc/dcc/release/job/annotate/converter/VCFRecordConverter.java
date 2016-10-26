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
package org.icgc.dcc.release.job.annotate.converter;

import java.io.File;
import java.util.Map;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import net.sf.picard.reference.IndexedFastaSequenceFile;

import org.broadinstitute.variant.variantcontext.VariantContext;
import org.broadinstitute.variant.variantcontext.VariantContextBuilder;
import org.broadinstitute.variant.vcf.VCFEncoder;
import org.broadinstitute.variant.vcf.VCFFormatHeaderLine;
import org.broadinstitute.variant.vcf.VCFHeader;
import org.broadinstitute.variant.vcf.VCFHeaderVersion;
import org.icgc.dcc.release.core.resolver.ReferenceGenomeResolver;
import org.icgc.dcc.release.job.annotate.converter.ICGCToVCFConverter.MutationType;
import org.icgc.dcc.release.job.annotate.util.Alleles;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

@RequiredArgsConstructor
public class VCFRecordConverter {

  /**
   * Dependencies.
   */
  private final VCFEncoder encoder;
  private final ICGCToVCFConverter converter;

  @SneakyThrows
  public VCFRecordConverter(
      @NonNull final File resourceDir,
      @NonNull final String resourceUrl,
      @NonNull final String refGenomeVersion) {
    this.encoder = new VCFEncoder(createVCFHeader(), true);
    val referenceGenome = resolveReferenceGenome(resourceDir, resourceUrl, refGenomeVersion);
    this.converter = new ICGCToVCFConverter(new IndexedFastaSequenceFile(referenceGenome));
  }

  public String createVCFRecord(
      @NonNull String chromosome,
      long start,
      long end,
      @NonNull String mutation,
      @NonNull MutationType type,
      @NonNull String reference,
      @NonNull String id) {
    val variant = createVariant(chromosome, start, end, mutation, type, reference, id);
    val line = encoder.encode(variant);

    return line;
  }

  private VariantContext createVariant(String chromosome, long start, long end, String mutation, MutationType type,
      String reference, String id) {
    val converted = converter.convert(chromosome, start, end, mutation, type, reference);

    return new VariantContextBuilder()
        .chr(chromosome)
        .start(converted.pos)
        .stop(converted.pos + converted.ref.length() - 1)
        .attributes(createAttribute("PRIM", id))
        .alleles(Alleles.createAlleles(converted.ref, converted.alt))
        .genotypes(converted.genotype)
        .make();
  }

  private File resolveReferenceGenome(@NonNull final File resourceDir,
      @NonNull final String resourceUrl,
      @NonNull final String refGenomeVersion) {
    val resolver = new ReferenceGenomeResolver(resourceDir, resourceUrl, refGenomeVersion);

    return resolver.resolve();
  }

  private static Map<String, Object> createAttribute(String key, Object value) {
    // VariantContextBuilder requires it to be mutable
    val attributes = Maps.<String, Object> newHashMap();
    attributes.put(key, value);

    return attributes;
  }

  private static VCFHeader createVCFHeader() {
    return new VCFHeader(
        ImmutableSet.of(
            new VCFFormatHeaderLine("<ID=GT,Number=1,Type=String,Description=\"Genotype\">", VCFHeaderVersion.VCF4_1)),
        ImmutableList.of("Patient_01_Germline", "Patient_01_Somatic"));
  }

}
