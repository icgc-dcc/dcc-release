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
package org.icgc.dcc.release.job.annotate.converter;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.core.util.Joiners.TAB;
import static org.icgc.dcc.release.job.annotate.util.VCFCodecs.createDecoder;

import java.util.List;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.broadinstitute.variant.vcf.VCFCodec;
import org.icgc.dcc.release.job.annotate.model.AnnotatedFileType;
import org.icgc.dcc.release.job.annotate.model.SecondaryEntity;
import org.junit.Test;

@Slf4j
public class SnpEffVCFToICGCConverterTest {

  private static final String GENE_BUILD_VERSION = "75";
  private static final VCFCodec DECODER = createDecoder();
  private static final AnnotatedFileType FILE_TYPE = AnnotatedFileType.SSM;
  private static final String HEADER = TAB.join("11", "32413565", ".", "C", "G", ".", ".", "");
  private static final String TAIL = TAB.join("GT", "0/0", "1/1");

  private static final String NO_ANNOTATION = "PRIM=123";
  private static final String NO_ID = "INFO=zzz";

  private SnpEffVCFToICGCConverter converter = new SnpEffVCFToICGCConverter(GENE_BUILD_VERSION);

  @Test
  public void testConvert_noId() throws Exception {
    val result = convert(NO_ID);
    assertThat(result).isEmpty();
  }

  @Test
  public void testConvert_noAnnotation() throws Exception {
    val result = convert(NO_ANNOTATION);
    assertThat(result).isEmpty();
  }

  private List<SecondaryEntity> convert(String annotation) {
    val line = createVCFRecord(annotation);
    val variant = DECODER.decode(line);
    val result = converter.convert(variant, FILE_TYPE);
    log.info("{}", result);

    return result;
  }

  private static String createVCFRecord(String annotation) {
    return format("%s%s\t%s", HEADER, annotation, TAIL);
  }

}
