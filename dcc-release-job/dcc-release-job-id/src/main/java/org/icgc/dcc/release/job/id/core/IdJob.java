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
package org.icgc.dcc.release.job.id.core;

import static com.google.common.base.Strings.isNullOrEmpty;
import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.id.client.core.IdClientFactory;
import org.icgc.dcc.id.client.http.HttpIdClient;
import org.icgc.dcc.id.client.http.HttpIdClient.Config;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.job.GenericJob;
import org.icgc.dcc.release.core.job.JobContext;
import org.icgc.dcc.release.core.job.JobType;
import org.icgc.dcc.release.job.id.config.IdentifierProperties;
import org.icgc.dcc.release.job.id.task.AddSurrogateDonorIdTask;
import org.icgc.dcc.release.job.id.task.AddSurrogateMutationIdTask;
import org.icgc.dcc.release.job.id.task.AddSurrogateSampleIdTask;
import org.icgc.dcc.release.job.id.task.AddSurrogateSpecimenIdTask;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class IdJob extends GenericJob {

  /**
   * Dependencies.
   */
  @Autowired
  IdentifierProperties identifierProperties;

  @Override
  public JobType getType() {
    return JobType.ID;
  }

  @Override
  public void execute(@NonNull JobContext jobContext) {
    clean(jobContext);
    id(jobContext);
  }

  private void clean(JobContext jobContext) {
    delete(jobContext,
        FileType.DONOR_SURROGATE_KEY,
        FileType.SPECIMEN_SURROGATE_KEY,
        FileType.SAMPLE_SURROGATE_KEY,
        FileType.SSM_P_MASKED_SURROGATE_KEY);
  }

  private void id(JobContext jobContext) {
    val releaseName = jobContext.getReleaseName();
    val idClientFactory = createIdClientFactory(releaseName);

    jobContext.execute(
        new AddSurrogateDonorIdTask(idClientFactory),
        new AddSurrogateSpecimenIdTask(idClientFactory),
        new AddSurrogateSampleIdTask(idClientFactory),
        new AddSurrogateMutationIdTask(idClientFactory));
  }

  private IdClientFactory createIdClientFactory(String release) {
    return new IdClientFactory(resolveIdentifierClassName(), createConfig(release, identifierProperties));
  }

  private String resolveIdentifierClassName() {
    val identifierClassName = identifierProperties.getClassname();

    return isNullOrEmpty(identifierClassName) ? HttpIdClient.class.getName() : identifierClassName;
  }

  private static Config createConfig(String release, IdentifierProperties identifierProperties) {
    return Config.builder()
        .serviceUrl(identifierProperties.getUrl())
        .release(release)
        .authToken(resolveToken(identifierProperties))
        .strictSSLCertificates(identifierProperties.isStrictSSLCertificates())
        .requestLoggingEnabled(identifierProperties.isRequestLoggingEnabled())
        .build();
  }

  /**
   * {@link HttpIdClient} assumes that token is not set when it's null.
   */
  private static String resolveToken(IdentifierProperties identifierProperties) {
    val token = identifierProperties.getToken();

    return isNullOrEmpty(token) ? null : token;
  }

}
