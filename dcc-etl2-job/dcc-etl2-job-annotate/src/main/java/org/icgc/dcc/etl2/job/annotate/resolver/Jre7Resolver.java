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
package org.icgc.dcc.etl2.job.annotate.resolver;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.net.HttpHeaders.COOKIE;
import static com.google.common.net.HttpHeaders.LOCATION;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.zip.GZIPInputStream;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.tools.tar.TarEntry;
import org.apache.tools.tar.TarInputStream;
import org.icgc.dcc.etl2.core.resolver.DirectoryResourceResolver;

/**
 * Downloads and installs Oracle's Java JRE 7
 * 
 * @see http://stackoverflow.com/questions/10268583/how-to-automate-download-and-installation-of-java-jdk-on-linux
 */
@Slf4j
public class Jre7Resolver extends DirectoryResourceResolver<File> {

  /**
   * The base URL of JDK installation.
   */
  private static final String JAVA_BINARY_NAME = "java";
  private static final String JRE_VERSION = "jdk-7u60";
  private static final String JRE_TARBALL_PREFIX = "jre-7u60";
  private static final String JRE_URL = "https://edelivery.oracle.com/otn-pub/java/jdk/7u60-b19/";

  public Jre7Resolver(@NonNull File resourceDir) {
    super(resourceDir);
  }

  @Override
  @SneakyThrows
  protected File get() {
    init();

    val version = JRE_VERSION;
    val downloadUrl = getDownloadUrl();
    val http = connect(downloadUrl);

    val dataDir = new File(getResourceDir(), "jre");
    val versionDir = new File(dataDir, version);
    if (versionDir.exists()) {
      return findJava(versionDir);
    }

    checkState(versionDir.mkdirs(), "Could not make data version directory '%s'", versionDir);

    log.info("Downloading JRE '{}'...", version);
    download(http.getInputStream(), versionDir);
    log.info("Finished downloading JRE");

    return findJava(versionDir);
  }

  private void download(InputStream inputStream, File downloadDir) throws IOException, FileNotFoundException {
    @Cleanup
    val tar = new TarInputStream(new GZIPInputStream(inputStream));

    TarEntry entry = null;
    while ((entry = tar.getNextEntry()) != null) {
      if (entry.isDirectory()) {
        val dir = new File(downloadDir, entry.getName());
        checkState(dir.mkdirs(), "Could not make dirs for '%s'", dir);
      } else {
        val name = entry.getName();
        val file = new File(downloadDir, entry.getName());

        @Cleanup
        val output = new FileOutputStream(file);

        log.info("Extracting '{}' to '{}'...", name, file);
        tar.copyEntryContents(output);

        if (JAVA_BINARY_NAME.equals(file.getName())) {
          file.setExecutable(true);
        }
      }
    }
  }

  @SneakyThrows
  public String getDownloadUrl() {
    val os = System.getProperty("os.name");
    val tarballFileName =
        os.equals("Mac OS X") ? JRE_TARBALL_PREFIX + "-macosx-x64.tar.gz" : JRE_TARBALL_PREFIX + "-linux-x64.tar.gz";
    val http = connect(JRE_URL + tarballFileName);
    val redirect = http.getHeaderField(LOCATION);

    return redirect;
  }

  private void init() throws NoSuchAlgorithmException, KeyManagementException {
    val sslConext = SSLContext.getInstance("TLS");
    sslConext.init(null, new TrustManager[] { new TrustAllX509TrustManager() }, new SecureRandom());
    HttpsURLConnection.setDefaultSSLSocketFactory(sslConext.getSocketFactory());
    HttpsURLConnection.setDefaultHostnameVerifier(new AllHostnameVerifier());
  }

  private HttpURLConnection connect(String url) throws MalformedURLException, IOException {
    val http = (HttpURLConnection) new URL(url).openConnection();
    http.setRequestProperty(COOKIE, "oraclelicense=accept-securebackup-cookie");
    http.setInstanceFollowRedirects(true);
    http.connect();

    return http;
  }

  private File findJava(File root) {
    val list = root.listFiles();

    if (list == null) return null;

    for (val file : list) {
      if (file.isDirectory()) {
        val result = findJava(file);
        if (result != null) {
          return result;
        }
      } else if (file.getName().equals(JAVA_BINARY_NAME)) {
        return file;
      }
    }

    return null;
  }

  private static class AllHostnameVerifier implements HostnameVerifier {

    @Override
    public boolean verify(String string, SSLSession ssls) {
      return true;
    }

  }

  private static class TrustAllX509TrustManager implements X509TrustManager {

    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return new X509Certificate[0];
    }

    @Override
    public void checkClientTrusted(X509Certificate[] certs, String authType) {
      // No-op
    }

    @Override
    public void checkServerTrusted(X509Certificate[] certs, String authType) {
      // No-op
    }

  }

}