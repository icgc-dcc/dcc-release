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
package org.icgc.dcc.release.job.image.core;

import static java.util.Arrays.asList;

import java.io.File;

import lombok.val;

import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.job.image.config.ImageProperties;
import org.icgc.dcc.release.test.job.AbstractJobTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class ImageJobTest extends AbstractJobTest {

  private static final String TCGA_PROJECT = "BLCA-US";
  private static final String NON_TCGA_PROJECT = "PACA-CA";

  /**
   * Class under test.
   */
  ImageJob job;

  @Override
  @Before
  public void setUp() {
    super.setUp();
    val properties = new ImageProperties().setSkipUrls(true);
    this.job = new ImageJob(properties);
  }

  @Test
  @Ignore
  public void testExecute() {
    given(new File(INPUT_TEST_FIXTURES_DIR));
    val jobContext = createJobContext(job.getType(), asList(TCGA_PROJECT, NON_TCGA_PROJECT));
    job.execute(jobContext);

    verifyResult(TCGA_PROJECT, FileType.SPECIMEN_SURROGATE_KEY_IMAGE);
    verifyResult(NON_TCGA_PROJECT, FileType.SPECIMEN_SURROGATE_KEY_IMAGE);
  }

}
