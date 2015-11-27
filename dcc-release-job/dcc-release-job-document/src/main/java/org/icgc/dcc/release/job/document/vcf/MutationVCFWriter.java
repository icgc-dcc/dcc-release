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
package org.icgc.dcc.release.job.document.vcf;

import static com.google.common.base.Throwables.propagate;
import static org.broadinstitute.variant.variantcontext.writer.VariantContextWriterFactory.create;
import static org.broadinstitute.variant.vcf.VCFHeaderLineCount.UNBOUNDED;
import static org.icgc.dcc.release.job.document.vcf.util.FeatureUtils.asMap;
import static org.icgc.dcc.release.job.document.vcf.util.FeatureUtils.getPropertyNames;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;
import net.sf.picard.reference.IndexedFastaSequenceFile;
import net.sf.samtools.SAMSequenceDictionary;

import org.broadinstitute.variant.variantcontext.Allele;
import org.broadinstitute.variant.variantcontext.VariantContextBuilder;
import org.broadinstitute.variant.variantcontext.writer.Options;
import org.broadinstitute.variant.variantcontext.writer.VariantContextWriter;
import org.broadinstitute.variant.vcf.VCFHeader;
import org.broadinstitute.variant.vcf.VCFHeaderLine;
import org.broadinstitute.variant.vcf.VCFHeaderLineType;
import org.broadinstitute.variant.vcf.VCFInfoHeaderLine;
import org.icgc.dcc.release.job.document.vcf.model.Consequence;
import org.icgc.dcc.release.job.document.vcf.model.Feature;
import org.icgc.dcc.release.job.document.vcf.model.Mutation;
import org.icgc.dcc.release.job.document.vcf.model.Occurrence;
import org.icgc.dcc.release.job.document.vcf.util.ICGCToVCFMutationConverter;
import org.icgc.dcc.release.job.document.vcf.util.VCFMutation;
import org.joda.time.DateTime;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

@Slf4j
@RequiredArgsConstructor
public class MutationVCFWriter implements Closeable {

  /**
   * Versions.
   */
  private static final String REFERENCE_GENOME_VERSION = "GRCh37";
  private static final String GENE_MODEL_VERSION = "ENSEMBL75";

  /**
   * Constants.
   */
  private static final Iterable<String> CONSEQUENCE_SUBFIELDS = getPropertyNames(Consequence.class);
  private static final List<String> OCCURRENCE_SUBFIELDS = getPropertyNames(Occurrence.class);
  private static final Joiner SUBFIELD_JOINER = Joiner.on('|');

  /**
   * Metadata.
   */
  private final String releaseName;
  private final boolean open;
  private final int totalSsmTestedDonorCount;

  /**
   * State.
   */
  private final VariantContextWriter writer;
  private final IndexedFastaSequenceFile sequenceFile;
  private final ICGCToVCFMutationConverter converter;

  public MutationVCFWriter(@NonNull String releaseName, @NonNull File fastaFile, @NonNull OutputStream outputStream,
      boolean open, int totalSsmTestedDonorCount) throws FileNotFoundException {
    this.releaseName = releaseName;
    this.sequenceFile = new IndexedFastaSequenceFile(fastaFile);
    this.converter = new ICGCToVCFMutationConverter(sequenceFile);
    this.writer = createWriter(sequenceFile.getSequenceDictionary(), outputStream);
    this.open = open;
    this.totalSsmTestedDonorCount = totalSsmTestedDonorCount;
  }

  public void writeHeader() {
    // @formatter:off
    val header = new VCFHeader();
    
    header.addMetaDataLine(new VCFHeaderLine("source",        releaseName));
    header.addMetaDataLine(new VCFHeaderLine("fileDate",      new DateTime().toString())); // Render date-time as an ISO 8601 string. The "toString" method on DateTime defaults to a built-in ISO 8601 formatte
    header.addMetaDataLine(new VCFHeaderLine("reference",     REFERENCE_GENOME_VERSION));
    header.addMetaDataLine(new VCFHeaderLine("geneModel",     GENE_MODEL_VERSION));
    header.addMetaDataLine(new VCFHeaderLine("comment",       "ICGC " + (open ? "open" : "closed") +  " access Simple Somatic Mutations (SSM) data dump in VCF format"));
    
    header.addMetaDataLine(new VCFInfoHeaderLine("mutation",        1, VCFHeaderLineType.String,  "Somatic mutation definition"));
    header.addMetaDataLine(new VCFInfoHeaderLine("affected_donors", 1, VCFHeaderLineType.Integer, "Number of donors with the current mutation"));
    header.addMetaDataLine(new VCFInfoHeaderLine("tested_donors",   1, VCFHeaderLineType.Integer, "Total number of donors with SSM data available"));
    header.addMetaDataLine(new VCFInfoHeaderLine("project_count",   1, VCFHeaderLineType.Integer, "Number of projects with the current mutation"));
    
    header.addMetaDataLine(new VCFInfoHeaderLine("CONSEQUENCE",    UNBOUNDED, VCFHeaderLineType.String,  "Mutation consequence predictions annotated by SnpEff (subfields: " + formatValues(CONSEQUENCE_SUBFIELDS) + ")"));
    header.addMetaDataLine(new VCFInfoHeaderLine("OCCURRENCE",     UNBOUNDED, VCFHeaderLineType.String,  "Mutation occurrence counts broken down by project (subfields: "    + formatValues(OCCURRENCE_SUBFIELDS)  + ")"));
    // @formatter:on

    writer.writeHeader(header);
  }

  public void writeFeature(Feature feature) {
    VCFMutation converted = converter.convert(
        feature.getChromosome(),
        feature.getChromosomeStart(),
        feature.getChromosomeEnd(),
        feature.getMutation().getMutation(),
        feature.getMutation().getMutationType(),
        feature.getMutation().getReference());

    try {
      writer.add(new VariantContextBuilder()
          .chr(feature.getChromosome())
          .start(converted.pos)
          .stop(converted.pos + converted.ref.length() - 1)
          .id(feature.getId())
          .alleles(createAlleles(converted.ref, converted.alt))
          .attributes(createAttributes(feature.getMutation(), totalSsmTestedDonorCount))
          .make());
    } catch (Exception e) {
      log.error("Exception writing feature " + feature + " with converted value + " + converted + ":", e);
      propagate(e);
    }
  }

  @Override
  public void close() throws IOException {
    writer.close();
    sequenceFile.close();
  }

  private static Map<String, Object> createAttributes(Mutation mutation, int totalSsmTestedDonorCount) {
    val attributes = ImmutableMap.<String, Object> builder();

    // @formatter:off
    attributes.put("mutation",        mutation.getMutation());
    attributes.put("affected_donors", mutation.getAffectedDonors());
    attributes.put("tested_donors",   totalSsmTestedDonorCount);
    attributes.put("project_count",   mutation.getProjectCount());

    attributes.put("CONSEQUENCE", formatList(mutation.getConsequences()));
    attributes.put("OCCURRENCE",  formatList(mutation.getOccurrences()));
    // @formatter:on

    return attributes.build();
  }

  private static List<Allele> createAlleles(String refAllele, Iterable<String> altAlleles) {
    val reference = true;
    val alleles = ImmutableList.<Allele> builder();
    alleles.add(Allele.create(refAllele, reference));
    for (val altAllele : altAlleles) {
      alleles.add(Allele.create(altAllele, !reference));
    }

    return alleles.build();
  }

  private static VariantContextWriter createWriter(SAMSequenceDictionary dictionary, OutputStream outputStream) {
    return create(outputStream, dictionary, EnumSet.noneOf(Options.class));
  }

  private static List<String> formatList(List<?> list) {
    val results = ImmutableList.<String> builder();
    for (val consequence : list) {
      val map = asMap(consequence);
      results.add(formatValues(map.values()));
    }

    return results.build();
  }

  private static String formatValues(Iterable<?> values) {
    return SUBFIELD_JOINER.join(values);
  }

}
