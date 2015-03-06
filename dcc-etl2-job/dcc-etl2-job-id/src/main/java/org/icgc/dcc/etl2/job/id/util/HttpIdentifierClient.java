/*
 * Copyright (c) 2013 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.etl2.job.id.util;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URI;

import javax.annotation.concurrent.ThreadSafe;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.methods.GetMethod;

@Slf4j
@ThreadSafe
public class HttpIdentifierClient implements IdentifierClient {

  private final HttpClient http;
  private final String release;

  private final MultiThreadedHttpConnectionManager connManager;

  private final static String DONOR_ID_PATH = "/api/donor/id";
  private final static String MUTATION_ID_PATH = "/api/mutation/id";
  private final static String PROJECT_ID_PATH = "/api/project/id";
  private final static String SAMPLE_ID_PATH = "/api/sample/id";
  private final static String SPECIMEN_ID_PATH = "/api/specimen/id";
  private final static int BUFFER_SIZE = 512;

  public HttpIdentifierClient(@NonNull String serviceUri, @NonNull String release) {
    this.release = release;
    connManager = new MultiThreadedHttpConnectionManager();
    try {
      URI service = new URI(serviceUri);
      service.getScheme();
      http = new HttpClient(connManager);
      http.getHostConfiguration().setHost(service.getHost(), service.getPort(), service.getScheme());
    } catch (Exception e) {
      log.error("Fail to parse uri for identifier from configuration file: {}", serviceUri);
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getDonorId(@NonNull String submittedDonorId, @NonNull String submittedProjectId) {
    NameValuePair[] params =
    {
        new NameValuePair("submittedDonorId", submittedDonorId),
        new NameValuePair("submittedProjectId", submittedProjectId),
        new NameValuePair("release", release)
    };
    return getBody(DONOR_ID_PATH, params);
  }

  public String getProjectId(@NonNull String submittedProjectId) {
    NameValuePair[] params =
    {
        new NameValuePair("submittedProjectId", submittedProjectId),
        new NameValuePair("release", release)
    };
    return getBody(PROJECT_ID_PATH, params);
  }

  @Override
  public String getMutationId(@NonNull String chromosome, @NonNull String chromosomeStart,
      @NonNull String chromosomeEnd, @NonNull String mutation, @NonNull String mutationType,
      @NonNull String assemblyVersion) {
    NameValuePair[] params =
    {
        new NameValuePair("chromosome", chromosome),
        new NameValuePair("chromosomeStart", chromosomeStart),
        new NameValuePair("chromosomeEnd", chromosomeEnd),
        new NameValuePair("mutation", mutation),
        new NameValuePair("mutationType", mutationType),
        new NameValuePair("assemblyVersion", assemblyVersion),
        new NameValuePair("release", release)
    };
    return getBody(MUTATION_ID_PATH, params);
  }

  @Override
  public String getSampleId(@NonNull String submittedSampleId, @NonNull String submittedProjectId) {
    NameValuePair[] params =
    {
        new NameValuePair("submittedSampleId", submittedSampleId),
        new NameValuePair("submittedProjectId", submittedProjectId),
        new NameValuePair("release", release)
    };
    return getBody(SAMPLE_ID_PATH, params);
  }

  @Override
  public String getSpecimenId(@NonNull String submittedSpecimenId, @NonNull String submittedProjectId) {
    NameValuePair[] params =
    {
        new NameValuePair("submittedSpecimenId", submittedSpecimenId),
        new NameValuePair("submittedProjectId", submittedProjectId),
        new NameValuePair("release", release)
    };
    return getBody(SPECIMEN_ID_PATH, params);
  }

  @Override
  public void close() {
    MultiThreadedHttpConnectionManager.shutdownAll();
  }

  @SneakyThrows
  private String getBody(String path, NameValuePair[] params) {
    GetMethod m = new GetMethod(path);
    try {
      m.setQueryString(params);
      int status = http.executeMethod(m);
      checkState(
          status == HttpStatus.SC_OK,
          "Unable to retrieve ids from dcc-identifier service using '%s' and '%s'",
          path, newArrayList(params));

      InputStream in = m.getResponseBodyAsStream();
      checkState(in != null, "No content returns from dcc-identifier service");
      ByteArrayOutputStream out = new ByteArrayOutputStream(BUFFER_SIZE * 2);
      byte[] buffer = new byte[BUFFER_SIZE];
      int len = 0;
      while ((len = in.read(buffer)) > 0) {
        out.write(buffer, 0, len);
      }
      out.close();
      return new String(out.toByteArray(), UTF_8);
    } finally {
      m.releaseConnection();
    }
  }

}
