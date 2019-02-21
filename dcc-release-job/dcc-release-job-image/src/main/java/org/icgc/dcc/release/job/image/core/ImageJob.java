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
package org.icgc.dcc.release.job.image.core;

import java.util.Collections;
import java.util.Map;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.job.GenericJob;
import org.icgc.dcc.release.core.job.JobContext;
import org.icgc.dcc.release.core.job.JobType;
import org.icgc.dcc.release.job.image.config.ImageProperties;
import org.icgc.dcc.release.job.image.task.AddSpecimenImageTask;
import org.icgc.dcc.release.job.image.util.SpecimenImageResolver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor(onConstructor = @__({ @Autowired}))
public class ImageJob extends GenericJob {

  /**
   * Dependencies
   */
  @NonNull
  private final ImageProperties properties;

  @Override
  public JobType getType() {
    return JobType.IMAGE;
  }

  @Override
  public void execute(@NonNull JobContext jobContext) {
    clean(jobContext);

    val specimenImageUrls = properties.isSkipUrls() ? Collections.<String, String>emptyMap() : getSpecimenImageUrls();
    addSpecimenImage(jobContext, specimenImageUrls);
  }

  private void clean(JobContext jobContext) {
    delete(jobContext, FileType.SPECIMEN_SURROGATE_KEY_IMAGE);
  }

  private void addSpecimenImage(JobContext jobContext, Map<String, String> specimenImageUrls) {
    val task = creatTask(specimenImageUrls);

    jobContext.execute(task);
  }

  private static Map<String, String> getSpecimenImageUrls() {
    return new SpecimenImageResolver(true).resolveUrls();
  }

  private static AddSpecimenImageTask creatTask(Map<String, String> specimenImageUrls) {
    return new AddSpecimenImageTask(specimenImageUrls);
  }

}
