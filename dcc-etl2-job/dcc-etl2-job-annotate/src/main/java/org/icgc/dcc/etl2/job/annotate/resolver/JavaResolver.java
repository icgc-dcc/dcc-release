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
package org.icgc.dcc.etl2.job.annotate.resolver;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;

import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolves location of java executable by JAVA_HOME or location on the PATH.
 */
@Slf4j
public class JavaResolver {

  private static final String COMMAND = "which java";

  public File resolve() {
    val javaHome = System.getenv("JAVA_HOME");
    val java = isNullOrEmpty(javaHome) ? resolveJavaFromPath() : resolveJavaFromJavaHome(javaHome);
    checkState(!isNullOrEmpty(java), "Failed to resolve java executable.");

    val javaExecutable = new File(java);
    checkState(javaExecutable.exists(), "Java executable %s does not exist", java);
    log.info("Resolved Java executable: {}", javaExecutable);

    return javaExecutable;
  }

  private String resolveJavaFromJavaHome(String javaHome) {
    return javaHome + "/jre/bin/java";
  }

  @SneakyThrows
  private String resolveJavaFromPath() {
    val process = Runtime.getRuntime().exec(COMMAND);
    process.waitFor();

    @Cleanup
    val reader = new BufferedReader(new InputStreamReader(process.getInputStream()));

    return reader.readLine();
  }

}
