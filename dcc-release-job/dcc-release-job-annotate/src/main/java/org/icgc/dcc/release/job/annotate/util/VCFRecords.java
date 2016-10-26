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

import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_MUTATION;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_OBSERVATION_VARIANT_ALLELE;
import static org.icgc.dcc.release.core.util.Mutations.createMutation;
import static org.icgc.dcc.release.job.annotate.model.AnnotatedFileType.SSM;

import java.io.File;
import java.io.OutputStream;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;

import org.broadinstitute.variant.variantcontext.writer.VariantContextWriterFactory;
import org.broadinstitute.variant.vcf.VCFHeader;
import org.broadinstitute.variant.vcf.VCFHeaderLine;
import org.icgc.dcc.release.job.annotate.converter.ICGCToVCFConverter.MutationType;
import org.icgc.dcc.release.job.annotate.model.AnnotatedFileType;
import org.icgc.dcc.release.job.annotate.model.VCFRecordContext;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

@NoArgsConstructor
public final class VCFRecords {

  private static final String TEMP_FILE_PREFIX = VCFRecords.class.getName() + "-";
  private static final String MISSING_ALLELE = "-";

  @SneakyThrows
  public static void writeVCFHeader(OutputStream stream) {
    // VariantContextWriterFactory requires a non-null FILE. Create any and delete it on exit
    val file = File.createTempFile(TEMP_FILE_PREFIX, null);
    file.deleteOnExit();

    val writer = VariantContextWriterFactory.create(file, stream, null);
    writer.writeHeader(createAnnotatedVCFHeader());
    stream.flush();

    deleteTempIndexFile(file);
  }

  public static VCFRecordContext getVCFRecordContext(@NonNull ObjectNode row, @NonNull AnnotatedFileType fileType) {
    val ref = row.get(fileType.getReferenceAlleleFieldName()).textValue();
    val reference = (ref.equals(MISSING_ALLELE)) ? "" : ref;

    return VCFRecordContext.builder()
        .chromosome(row.get(fileType.getChromosomeFieldName()).textValue())
        .start(row.get(fileType.getChromosomeStartFieldName()).asLong())
        .end(row.get(fileType.getChromosomeEndFieldName()).asLong())
        .mutation(getMutation(row, fileType))
        .type(MutationType.fromId(row.get(fileType.getMutationTypeFieldName()).textValue()))
        .reference(reference)
        .id(row.get(fileType.getObservationIdFieldName()).textValue())
        .build();
  }

  private static VCFHeader createAnnotatedVCFHeader() {
    return new VCFHeader(
        ImmutableSet.of(new VCFHeaderLine("PEDIGREE", "<Derived=Patient_01_Somatic,Original=Patient_01_Germline>")),
        ImmutableList.of("Patient_01_Germline", "Patient_01_Somatic"));
  }

  private static void deleteTempIndexFile(File orifinalFile) {
    val tmpFile = new File(orifinalFile.getAbsolutePath() + ".idx");
    tmpFile.deleteOnExit();
  }

  private static String getMutation(ObjectNode row, AnnotatedFileType fileType) {
    if (fileType == SSM) {
      return row.get(SUBMISSION_MUTATION).textValue();
    }

    val mutatedFrom = row.get(fileType.getReferenceAlleleFieldName()).textValue();
    val mutatedTo = row.get(SUBMISSION_OBSERVATION_VARIANT_ALLELE).textValue();

    return createMutation(mutatedFrom, mutatedTo);
  }

}
