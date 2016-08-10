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

import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MINUTES;

import java.io.File;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.broadinstitute.variant.variantcontext.VariantContext;
import org.broadinstitute.variant.variantcontext.VariantContextBuilder;
import org.broadinstitute.variant.variantcontext.writer.VariantContextWriterFactory;
import org.broadinstitute.variant.vcf.VCFEncoder;
import org.broadinstitute.variant.vcf.VCFFormatHeaderLine;
import org.broadinstitute.variant.vcf.VCFHeader;
import org.broadinstitute.variant.vcf.VCFHeaderLine;
import org.broadinstitute.variant.vcf.VCFHeaderVersion;
import org.icgc.dcc.release.core.config.SnpEffProperties;
import org.icgc.dcc.release.core.resolver.ReferenceGenomeResolver;
import org.icgc.dcc.release.job.annotate.converter.ICGCToVCFConverter;
import org.icgc.dcc.release.job.annotate.converter.ICGCToVCFConverter.MutationType;
import org.icgc.dcc.release.job.annotate.model.AnnotatedFileType;
import org.icgc.dcc.release.job.annotate.model.SecondaryEntity;
import org.icgc.dcc.release.job.annotate.resolver.JavaResolver;
import org.icgc.dcc.release.job.annotate.resolver.SnpEffDatabaseResolver;
import org.icgc.dcc.release.job.annotate.resolver.SnpEffJarResolver;
import org.icgc.dcc.release.job.annotate.util.Alleles;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;
import net.sf.picard.reference.IndexedFastaSequenceFile;

@Slf4j
@RequiredArgsConstructor
public class SnpEffPredictor {

  /**
   * Constants
   */
  private static final int PREDICTION_TIMEOUT_MINUTES = 5;

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
    this.stream = new PrintStream(process.getOutputStream(), false, UTF_8.name());
    this.encoder = new VCFEncoder(createVCFHeader(), true);
    this.converter = new ICGCToVCFConverter(new IndexedFastaSequenceFile(resolveReferenceGenome()));

    // Start handler threads
    executor.execute(new SnpEffResultHandler(process.getInputStream(), queue, fileType, properties
        .getGeneBuildVersion()));
    executor.execute(new SnpEffLogHandler(process.getErrorStream()));
    initializeSnpEff();
  }

  @SneakyThrows
  public List<SecondaryEntity> predict(String chromosome, long start, long end, String mutation, MutationType type,
      String reference, String id) {

    val variant = createVariant(chromosome, start, end, mutation, type, reference, id);
    val line = encoder.encode(variant);
    stream.println(line);
    stream.flush();

    // Temporary fix for DCC-4663 to allow the calling task to fail if the timeout is exceeded
    val predictions = queue.poll(PREDICTION_TIMEOUT_MINUTES, MINUTES);
    val timeout = predictions == null; // Will always be non-null under normal circumstances
    if (timeout) {
      checkState(false,
          "Timeout after waiting %s min for next prediction from SnpEff process. Exit code = %s",
          PREDICTION_TIMEOUT_MINUTES,
          process.isAlive() ? process.exitValue() : "<still running!>");
    }

    return predictions;
  }

  public void stop() throws InterruptedException {
    stream.close();
    log.info("Exit code: {}", process.waitFor());

    executor.shutdownNow();
    executor.awaitTermination(1, MINUTES);
  }

  @SneakyThrows
  private void initializeSnpEff() {
    log.warn("Initializing SnpEff...");

    // VariantContextWriterFactory requires a non-null FILE. Create any and delete it on exit
    val prefix = SnpEffPredictor.class.getName();
    val file = File.createTempFile(prefix, null);
    file.deleteOnExit();

    val writer = VariantContextWriterFactory.create(file, stream, null);
    writer.writeHeader(createAnnotatedVCFHeader());
    stream.flush();

    deleteTempFile(file);
  }

  private void deleteTempFile(File orifinalFile) {
    val tmpFile = new File(orifinalFile.getAbsolutePath() + ".idx");
    tmpFile.deleteOnExit();
  }

  private File resolveJava() {
    val resolver = new JavaResolver();

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

    return new VariantContextBuilder()
        .chr(chromosome)
        .start(converted.pos)
        .stop(converted.pos + converted.ref.length() - 1)
        .attributes(createAttribute("PRIM", id))
        .alleles(Alleles.createAlleles(converted.ref, converted.alt))
        .genotypes(converted.genotype)
        .make();
  }

  private static VCFHeader createVCFHeader() {
    return new VCFHeader(
        ImmutableSet.of(
            new VCFFormatHeaderLine("<ID=GT,Number=1,Type=String,Description=\"Genotype\">", VCFHeaderVersion.VCF4_1)),
        ImmutableList.of("Patient_01_Germline", "Patient_01_Somatic"));
  }

  private static VCFHeader createAnnotatedVCFHeader() {
    return new VCFHeader(
        ImmutableSet.of(new VCFHeaderLine("PEDIGREE", "<Derived=Patient_01_Somatic,Original=Patient_01_Germline>")),
        ImmutableList.of("Patient_01_Germline", "Patient_01_Somatic"));
  }

  private static Map<String, Object> createAttribute(String key, Object value) {
    // VariantContextBuilder requires it to be mutable
    val attributes = Maps.<String, Object> newHashMap();
    attributes.put(key, value);

    return attributes;
  }

}
