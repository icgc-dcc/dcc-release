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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.release.core.config.SnpEffProperties;
import org.icgc.dcc.release.job.annotate.converter.ICGCToVCFConverter.MutationType;
import org.icgc.dcc.release.job.annotate.converter.VCFRecordConverter;
import org.icgc.dcc.release.job.annotate.model.AnnotatedFileType;
import org.icgc.dcc.release.job.annotate.model.SecondaryEntity;
import org.icgc.dcc.release.job.annotate.resolver.JavaResolver;
import org.icgc.dcc.release.job.annotate.resolver.SnpEffDatabaseResolver;
import org.icgc.dcc.release.job.annotate.resolver.SnpEffJarResolver;
import org.icgc.dcc.release.job.annotate.util.VCFRecords;

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
  private VCFRecordConverter vcfRecordConverter;

  @SneakyThrows
  public void start() {
    this.process = new SnpEffProcess(resolveJar(), resolveJava(), resolveDataDir(), properties.getDatabaseVersion());
    this.stream = new PrintStream(process.getOutputStream(), false, UTF_8.name());
    this.vcfRecordConverter = new VCFRecordConverter(properties.getResourceDir(), properties.getResourceUrl(),
        properties.getReferenceGenomeVersion());

    // Start handler threads
    executor.execute(new SnpEffResultHandler(process.getInputStream(), queue, fileType, properties
        .getGeneBuildVersion()));
    executor.execute(new SnpEffLogHandler(process.getErrorStream()));
    initializeSnpEff();
  }

  @SneakyThrows
  public List<SecondaryEntity> predict(String chromosome, long start, long end, String mutation, MutationType type,
      String reference, String id) {
    val line = vcfRecordConverter.createVCFRecord(chromosome, start, end, mutation, type, reference, id);
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

  private void initializeSnpEff() {
    log.info("Initializing SnpEff...");
    VCFRecords.writeVCFHeader(stream);
    stream.flush();
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

}
