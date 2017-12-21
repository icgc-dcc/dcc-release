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
package org.icgc.dcc.release.core.util;

import static com.google.common.base.Strings.repeat;
import static lombok.AccessLevel.PRIVATE;

import javafx.util.Pair;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.slf4j.Logger;

import java.util.ArrayList;

@Slf4j
@NoArgsConstructor(access = PRIVATE)
public final class Loggers {

  public static void logWithHeader(@NonNull String message, Object... args) {
    logWithHeader(log, message, args);
  }

  public static void logWithHeader(@NonNull Logger log, @NonNull String message, Object... args) {
    printLine(log);
    log.info(message, args);
    printLine(log);
  }

  /**
   * Post array of key/values to url - very useful for logging from inside a
   * transform function where otherwise it's very difficult to debug
   * @param targetURL - IP/Endpoint to send POST request to
   * @param data - ArrayList of string/object pairs to be parsed into post data
   */
  public static void logToUrl(String targetURL, ArrayList<Pair<String, Object>> data) {

    HttpClient client = new HttpClient();
    PostMethod method = new PostMethod(targetURL);

    try {
      for (Pair<String, Object> keyVal : data) {
        method.addParameter(keyVal.getKey(), keyVal.getValue().toString());
      }
      client.executeMethod(method);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      method.releaseConnection();
    }
  }

  private static void printLine(Logger log) {
    log.info("{}", repeat("-", 100));
  }

}
