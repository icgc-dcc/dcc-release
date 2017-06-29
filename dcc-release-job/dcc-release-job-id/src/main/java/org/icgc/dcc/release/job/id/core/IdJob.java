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

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static org.icgc.dcc.common.core.util.Splitters.TAB;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.icgc.dcc.id.client.core.IdClientFactory;
import org.icgc.dcc.id.client.http.HttpIdClient;
import org.icgc.dcc.id.client.http.webclient.WebClientConfig;
import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.job.GenericJob;
import org.icgc.dcc.release.core.job.JobContext;
import org.icgc.dcc.release.core.job.JobType;
import org.icgc.dcc.release.job.id.config.IdProperties;
import org.icgc.dcc.release.job.id.config.PostgresqlProperties;
import org.icgc.dcc.release.job.id.dump.DumpDataToHDFS;
import org.icgc.dcc.release.job.id.dump.impl.DumpMutationDataByPGCopyManager;
import org.icgc.dcc.release.job.id.function.AddSurrogateSpecimenId;
import org.icgc.dcc.release.job.id.model.*;
import org.icgc.dcc.release.job.id.parser.ExportStringParser;
import org.icgc.dcc.release.job.id.task.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import lombok.NonNull;
import lombok.val;
import scala.reflect.ClassTag$;

import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class IdJob extends GenericJob {

  /**
   * Constants.
   */
  private static final String RELEASE_NAME_REGEX = "ICGC\\d+";

  /**
   * Dependencies.
   */
  @Autowired
  IdProperties identifierProperties;
  @Autowired
  PostgresqlProperties postgresqlProperties;

  static {
    try {
      Class.forName("org.postgresql.Driver");
    } catch (ClassNotFoundException e) {
      log.error(e.getMessage());
    }
  }

  @Override
  public JobType getType() {
    return JobType.ID;
  }

  @Override
  public void execute(@NonNull JobContext jobContext) {
    clean(jobContext);
    boolean bSuccessful = dumpPGDataToHDFS(jobContext);
    if(!bSuccessful) {
      throw new RuntimeException("Dumping postgresql data failed!");
    }
    id(jobContext);
  }

  private void clean(JobContext jobContext) {
    delete(jobContext,
        FileType.DONOR_SURROGATE_KEY,
        FileType.SPECIMEN_SURROGATE_KEY,
        FileType.SAMPLE_SURROGATE_KEY,
        FileType.SSM_P_MASKED_SURROGATE_KEY);
  }

  private DumpDataToHDFS getDumpImpl(JobContext jobContext) {
    return new DumpMutationDataByPGCopyManager(jobContext.getFileSystem(), this.postgresqlProperties, jobContext.getWorkingDir() + AddSurrogateMutationIdTask.mutationDumpPath);
  }

  private boolean dumpPGDataToHDFS(@NonNull JobContext jobContext) {
    DumpDataToHDFS dump = getDumpImpl(jobContext);
    return dump.dump();
  }

  private void id(JobContext jobContext) {
    val releaseName = resolveReleaseName(jobContext.getReleaseName());
    val idClientFactory = createIdClientFactory(releaseName);

    Broadcast<Map<SampleID, String>> samples = AddSurrogateSampleIdTask.createCache(jobContext, idClientFactory);

    Broadcast<Map<DonorID, String>> donors = AddSurrogateDonorIdTask.createCache(jobContext, idClientFactory);

    Broadcast<Map<SpecimenID, String>> specimens = AddSurrogateSpecimenIdTask.createCache(jobContext, idClientFactory);


    SQLContext sqlContext = new SQLContext(jobContext.getJavaSparkContext());

    DataFrame mutationDF = AddSurrogateMutationIdTask.createDataFrameForPGData(sqlContext, jobContext);

    jobContext.execute(
      new AddSurrogateSampleIdTask(idClientFactory, samples),
      new AddSurrogateDonorIdTask(idClientFactory, donors),
      new AddSurrogateSpecimenIdTask(idClientFactory, specimens),
      new AddSurrogateMutationIdTask(idClientFactory, mutationDF, sqlContext)
    );
  }

  private static String resolveReleaseName(String releaseName) {
    val idReleaseName = releaseName.toUpperCase().split("-")[0];
    checkState(idReleaseName.matches(RELEASE_NAME_REGEX), "Release name %s does not match regex %s", idReleaseName,
        RELEASE_NAME_REGEX);

    return idReleaseName;
  }

  private IdClientFactory createIdClientFactory(String release) {
    return new IdClientFactory(resolveIdentifierClassName(), createConfig(release, identifierProperties));
  }

  private String resolveIdentifierClassName() {
    val identifierClassName = identifierProperties.getClassname();

    return isNullOrEmpty(identifierClassName) ? HttpIdClient.class.getName() : identifierClassName;
  }

  private static WebClientConfig createConfig(String release, IdProperties identifierProperties) {
    val builder = WebClientConfig.builder()
        .serviceUrl(identifierProperties.getUrl())
        .release(release)
        .authToken(resolveToken(identifierProperties))
        .strictSSLCertificates(identifierProperties.isStrictSSLCertificates())
        .requestLoggingEnabled(identifierProperties.isRequestLoggingEnabled());

    if (identifierProperties.getMaxRetries() != 0) {
      builder.maxRetries(identifierProperties.getMaxRetries());
    }

    if (identifierProperties.getRetryMultiplier() != 0f) {
      builder.retryMultiplier(identifierProperties.getRetryMultiplier());
    }

    if (identifierProperties.getWaitBeforeRetrySeconds() != 0) {
      builder.waitBeforeRetrySeconds(identifierProperties.getWaitBeforeRetrySeconds());
    }

    return builder.build();
  }

  /**
   * {@link HttpIdClient} assumes that token is not set when it's null.
   */
  private static String resolveToken(IdProperties identifierProperties) {
    val token = identifierProperties.getToken();

    return isNullOrEmpty(token) ? null : token;
  }

}
