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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.icgc.dcc.common.hadoop.fs.FileSystems;

@RequiredArgsConstructor
public final class SaveVCFRecords implements VoidFunction<Iterator<String>> /*FlatMapFunction<Iterator<String>, Void>*/ {

    @NonNull
    private final String workingDir;
    @NonNull
    private final Map<String, String> fileSystemSettings;

    /**
     * See
     * https://wiki.oicr.on.ca/display/DCCSOFT/Aggregated+Data+Download+Specification?focusedCommentId=57774680#comment
     * -57774680
     */
    public static final String VCF_FILE_NAME = "simple_somatic_mutation.aggregated.vcf.gz";

    @Override
    public void call(Iterator<String> rows) throws Exception {
    val vcfFilePath = new Path(workingDir, SaveVCFRecords.VCF_FILE_NAME);
    val fileSystem = FileSystems.getFileSystem(fileSystemSettings);
    @Cleanup
    val writer = getWriter(vcfFilePath, fileSystem);
    while (rows.hasNext()) {
      writer.write(rows.next());
    }

//    return Collections.emptyList();
  }

  private static BufferedWriter getWriter(Path vcfFilePath, FileSystem fileSystem) throws IOException {
    return new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(fileSystem.create(vcfFilePath)), UTF_8));
  }

}