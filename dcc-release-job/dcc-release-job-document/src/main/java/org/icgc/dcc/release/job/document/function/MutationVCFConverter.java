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
package org.icgc.dcc.release.job.document.function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.Collections;
import java.util.Iterator;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.icgc.dcc.release.core.config.SnpEffProperties;
import org.icgc.dcc.release.core.resolver.ReferenceGenomeResolver;
import org.icgc.dcc.release.job.document.io.MutationVCFDocumentWriter;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;

@RequiredArgsConstructor
public final class MutationVCFConverter implements FlatMapFunction<Iterator<ObjectNode>, String> {

  private final int testedDonorCount;
  @NonNull
  private final String releaseName;
  @NonNull
  private final SnpEffProperties properties;

  private ByteArrayOutputStream buffer;
  private MutationVCFDocumentWriter mutationWriter;

  @Override
  public Iterable<String> call(Iterator<ObjectNode> iterator) throws Exception {
    initDependencies();

    val vcfRecords = Lists.<String> newArrayList();
    while (iterator.hasNext()) {
      mutationWriter.write(iterator.next());
      val record = buffer.toString("UTF-8");
      buffer.reset();
      checkState(!isNullOrEmpty(record), "A VCF record can't be empty");
      vcfRecords.add(record);
    }
    Collections.sort(vcfRecords);

    return vcfRecords;
  }

  @SneakyThrows
  private void initDependencies() {
    buffer = new ByteArrayOutputStream();
    val fastaFile = resolveFastaFile();
    mutationWriter = new MutationVCFDocumentWriter(releaseName, fastaFile, buffer, testedDonorCount);
    // Resetting buffer, because when MutationVCFDocumentWriter it writes a VCF header to the output stream(buffer)
    // which is not required here and will be created separately.
    buffer.reset();
  }

  private File resolveFastaFile() {
    val resolver = new ReferenceGenomeResolver(
        properties.getResourceDir(),
        properties.getResourceUrl(),
        properties.getReferenceGenomeVersion());

    return resolver.resolve();
  }

}