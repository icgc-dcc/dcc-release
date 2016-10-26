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

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.common.core.io.Files2.getCompressionAgnosticBufferedReader;
import static org.icgc.dcc.release.job.annotate.util.VCFRecords.getVCFRecordContext;
import static org.icgc.dcc.release.job.annotate.util.VCFRecords.writeVCFHeader;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;

import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.release.core.util.JacksonFactory;
import org.icgc.dcc.release.job.annotate.converter.VCFRecordConverter;
import org.icgc.dcc.release.job.annotate.model.AnnotatedFileType;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * This utility converts variants from the ICGC(JSON) format to VCF format.
 */
@Slf4j
public class ICGCtoVCFFileConverter {

  /**
   * Constants.
   */
  private static final String REFERENCE_GENOME_VERSION = "GRCh37.75.v1";
  private static final File RESOURCE_DIR = new File("/tmp/dcc-release");
  private static final AnnotatedFileType ANNOTATED_FILE_TYPE = AnnotatedFileType.SSM;

  private static final String RESOURCE_URL =
      "https://artifacts.oicr.on.ca/artifactory/simple/dcc-dependencies/org/icgc/dcc";

  /**
   * Configuration.
   */
  private final File inputFile;
  private final File outputFile;
  private final AnnotatedFileType fileType;

  /**
   * Dependencies.
   */
  private VCFRecordConverter vcfRecordConverter;

  private ICGCtoVCFFileConverter(File inputFile, File outputFile, AnnotatedFileType fileType) {
    this.inputFile = inputFile;
    this.outputFile = outputFile;
    this.vcfRecordConverter = new VCFRecordConverter(RESOURCE_DIR, RESOURCE_URL, REFERENCE_GENOME_VERSION);
    this.fileType = ANNOTATED_FILE_TYPE;
  }

  public static void main(String[] args) throws Exception {
    checkArguments(args);
    val inputFile = new File(args[0]);
    checkState(inputFile.exists() && inputFile.canRead(), "Input file is not readable.");
    val outputFile = new File(args[1]);
    if (!outputFile.exists()) {
      checkState(outputFile.createNewFile(), "Failed to create file %s", outputFile);
    }

    checkState(outputFile.canWrite(), "Output file is not writeable");

    new ICGCtoVCFFileConverter(inputFile, outputFile, ANNOTATED_FILE_TYPE).convert();
  }

  @SneakyThrows
  private void convert() {
    @Cleanup
    val inputReader = getCompressionAgnosticBufferedReader(inputFile.toString());
    val fileOutputStream = new FileOutputStream(outputFile);
    writeVCFHeader(fileOutputStream);
    fileOutputStream.flush();

    @Cleanup
    val outputWriter = new BufferedWriter(new OutputStreamWriter(fileOutputStream));

    String line = null;
    while ((line = inputReader.readLine()) != null) {
      val vcfRecord = convertToVCF(line);
      outputWriter.write(vcfRecord);
      outputWriter.newLine();
    }

    outputWriter.flush();
  }

  @SneakyThrows
  private String convertToVCF(String line) {
    final ObjectNode row = JacksonFactory.READER.readValue(line);
    val context = getVCFRecordContext(row, fileType);
    log.debug("Converting line: {}", line);

    return vcfRecordConverter.createVCFRecord(
        context.getChromosome(),
        context.getStart(),
        context.getEnd(),
        context.getMutation(),
        context.getType(),
        context.getReference(),
        context.getId());
  }

  private static void checkArguments(String[] args) {
    if (args.length != 2) {
      printUsage();
      System.exit(1);
    }
  }

  private static void printUsage() {
    log.info("\nUsage:\n\t{} <input_file> <output_file>", ICGCtoVCFFileConverter.class.getSimpleName());
  }

}
