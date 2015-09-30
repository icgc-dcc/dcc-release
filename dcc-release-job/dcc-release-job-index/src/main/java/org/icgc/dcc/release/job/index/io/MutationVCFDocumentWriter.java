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
package org.icgc.dcc.release.job.index.io;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import lombok.val;

import org.icgc.dcc.release.job.index.util.MutationCentricFeatureConverter;
import org.icgc.dcc.release.job.index.vcf.MutationVCFWriter;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class MutationVCFDocumentWriter {

  private final MutationVCFWriter writer;
  private final MutationCentricFeatureConverter converter = new MutationCentricFeatureConverter();

  public MutationVCFDocumentWriter(String releaseName, File fastaFile, OutputStream outputStream, int totalSsmTestedDonorCount) throws IOException {
    this.writer = new MutationVCFWriter(releaseName, fastaFile, outputStream, true, totalSsmTestedDonorCount);

    writer.writeHeader();
  }

  public void write(ObjectNode document) throws IOException {
    val feature = converter.convert(document);

    writer.writeFeature(feature);
  }

  public void close() throws IOException {
    writer.close();
  }

}
