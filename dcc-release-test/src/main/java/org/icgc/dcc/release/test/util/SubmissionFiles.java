/*
 * Copyright (c) 2015 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.release.test.util;

import java.util.List;

import lombok.experimental.UtilityClass;

import org.icgc.dcc.common.core.meta.FileCodeListsResolver;
import org.icgc.dcc.common.core.meta.FileDictionaryResolver;
import org.icgc.dcc.release.core.submission.SubmissionFileSchema;
import org.icgc.dcc.release.core.submission.SubmissionFileSchemas;
import org.icgc.dcc.release.core.submission.SubmissionMetadataService;

@UtilityClass
public class SubmissionFiles {

  private static final String TEST_FIXTURES_DIR = "../../dcc-release-test/src/main/resources/fixtures";
  private static final String DICTIONARY_FILE = TEST_FIXTURES_DIR + "/dictionary.json.gz";
  private static final String CODE_LISTS_FILE = TEST_FIXTURES_DIR + "/codelists.json.gz";

  public static SubmissionFileSchemas getSchemas() {
    return new SubmissionFileSchemas(SubmissionFiles.getMetadata());
  }

  public static List<SubmissionFileSchema> getMetadata() {
    return new SubmissionMetadataService(
        new FileDictionaryResolver(DICTIONARY_FILE),
        new FileCodeListsResolver(CODE_LISTS_FILE))
        .getMetadata();
  }

}
