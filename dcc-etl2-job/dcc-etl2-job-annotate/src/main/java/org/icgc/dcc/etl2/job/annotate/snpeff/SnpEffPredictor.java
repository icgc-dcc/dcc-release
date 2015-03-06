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
package org.icgc.dcc.etl2.job.annotate.snpeff;

import static java.util.concurrent.TimeUnit.MINUTES;

import java.io.File;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;
import net.sf.picard.reference.IndexedFastaSequenceFile;

import org.broadinstitute.variant.variantcontext.VariantContext;
import org.broadinstitute.variant.variantcontext.VariantContextBuilder;
import org.broadinstitute.variant.variantcontext.writer.VariantContextWriterFactory;
import org.broadinstitute.variant.vcf.VCFEncoder;
import org.broadinstitute.variant.vcf.VCFFormatHeaderLine;
import org.broadinstitute.variant.vcf.VCFHeader;
import org.broadinstitute.variant.vcf.VCFHeaderLine;
import org.broadinstitute.variant.vcf.VCFHeaderVersion;
import org.icgc.dcc.etl2.core.resolver.ReferenceGenomeResolver;
import org.icgc.dcc.etl2.job.annotate.config.SnpEffProperties;
import org.icgc.dcc.etl2.job.annotate.converter.ICGCToVCFConverter;
import org.icgc.dcc.etl2.job.annotate.converter.ICGCToVCFConverter.MutationType;
import org.icgc.dcc.etl2.job.annotate.model.AnnotatedFileType;
import org.icgc.dcc.etl2.job.annotate.model.SecondaryEntity;
import org.icgc.dcc.etl2.job.annotate.resolver.Jre7Resolver;
import org.icgc.dcc.etl2.job.annotate.resolver.SnpEffDatabaseResolver;
import org.icgc.dcc.etl2.job.annotate.resolver.SnpEffJarResolver;
import org.icgc.dcc.etl2.job.annotate.util.Alleles;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

@Slf4j
@RequiredArgsConstructor
public class SnpEffPredictor {

  /**
   * Configuration.
   */
  @NonNull
  private final SnpEffProperties properties;
  @NonNull
  private final AnnotatedFileType fileType;

  /**
   * State.
   */
  private BlockingQueue<List<SecondaryEntity>> queue = new ArrayBlockingQueue<List<SecondaryEntity>>(100);

  /**
   * Dependencies.
   */
  private SnpEffProcess process;
  private ExecutorService executor = Executors.newFixedThreadPool(2);
  private PrintStream stream;
  private VCFEncoder encoder;
  private ICGCToVCFConverter converter;

  @SneakyThrows
  public void start() {
    this.process = new SnpEffProcess(resolveJar(), resolveJava(), resolveDataDir(), properties.getDatabaseVersion());
    this.stream = new PrintStream(process.getOutputStream());
    this.encoder = new VCFEncoder(createVCFHeader(), true);
    this.converter = new ICGCToVCFConverter(new IndexedFastaSequenceFile(resolveReferenceGenome()));

    // Start handler threads
    executor.execute(new SnpEffResultHandler(process.getInputStream(), queue, fileType, properties
        .getGeneBuildVersion()));
    executor.execute(new SnpEffLogHandler(process.getErrorStream()));
    initializeSnpEff();
  }

  @SneakyThrows
  private void initializeSnpEff() {
    // VariantContextWriterFactory requires a non-null FILE. Create any and delete it on exit
    val prefix = "zzz";
    val file = File.createTempFile(prefix, null);
    val writer = VariantContextWriterFactory.create(file, stream, null);
    writer.writeHeader(createAnnotatedVCFHeader());
    stream.flush();
    deleteTempFile(file);
  }

  private void deleteTempFile(File orifinalFile) {
    val tmpFile = new File(orifinalFile.getAbsolutePath() + ".idx");
    tmpFile.deleteOnExit();
  }

  private static VCFHeader createAnnotatedVCFHeader() {
    Set<VCFHeaderLine> set = Sets.newHashSet();
    set.add(new VCFHeaderLine("PEDIGREE", "<Derived=Patient_01_Somatic,Original=Patient_01_Germline>"));

    return new VCFHeader(set, ImmutableList.of("Patient_01_Germline", "Patient_01_Somatic"));
  }

  @SneakyThrows
  public List<SecondaryEntity> predict(String chromosome, long start, long end, String mutation, MutationType type,
      String reference, String id) {

    val variant = createVariant(chromosome, start, end, mutation, type, reference, id);
    val line = encoder.encode(variant);
    stream.println(line);
    stream.flush();

    return queue.take();
  }

  public void stop() throws InterruptedException {
    stream.close();
    log.info("Exit code: {}", process.waitFor());

    executor.shutdownNow();
    executor.awaitTermination(1, MINUTES);
  }

  private File resolveJava() {
    val resolver = new Jre7Resolver(properties.getResourceDir());

    return resolver.resolve();
  }

  private File resolveJar() {
    val resolver = new SnpEffJarResolver(
        properties.getResourceDir(),
        properties.getVersion());

    return resolver.resolve();
  }

  private File resolveDataDir() {
    // TODO: Pass in common directory or create it based on referenceGenomeVersion
    val resolver = new SnpEffDatabaseResolver(
        properties.getResourceDir(),
        properties.getResourceUrl(),
        properties.getDatabaseVersion());

    return resolver.resolve();
  }

  private File resolveReferenceGenome() {
    val resolver = new ReferenceGenomeResolver(
        properties.getResourceDir(),
        properties.getResourceUrl(),
        properties.getReferenceGenomeVersion());

    return resolver.resolve();
  }

  private VariantContext createVariant(String chromosome, long start, long end, String mutation, MutationType type,
      String reference, String id) {
    val converted = converter.convert(chromosome, start, end, mutation, type, reference);
    val variant = new VariantContextBuilder()
        .chr(chromosome)
        .start(converted.pos)
        .stop(converted.pos + converted.ref.length() - 1)
        .attributes(createAttribute("PRIM", id))
        .alleles(Alleles.createAlleles(converted.ref, converted.alt))
        .genotypes(converted.genotype)
        .make();

    return variant;
  }

  private static VCFHeader createVCFHeader() {
    Set<VCFHeaderLine> set = Sets.newHashSet();
    val line =
        new VCFFormatHeaderLine("<ID=GT,Number=1,Type=String,Description=\"Genotype\">", VCFHeaderVersion.VCF4_1);
    set.add(line);

    return new VCFHeader(set, ImmutableList.of("Patient_01_Germline", "Patient_01_Somatic"));
  }

  private static Map<String, Object> createAttribute(String key, Object value) {
    // VariantContextBuilder requires it to be mutable
    Map<String, Object> result = Maps.newHashMap();
    result.put(key, value);

    return result;
  }

}
