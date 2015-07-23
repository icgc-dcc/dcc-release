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
package org.icgc.dcc.etl2.workflow.exec;

import org.icgc.dcc.etl2.core.job.Job;
import org.icgc.dcc.etl2.workflow.WorkflowMain;
import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.stream.slf4j.Slf4jOutputStream;
import org.zeroturnaround.exec.stream.slf4j.Slf4jStream;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JobProcessExecutor {

  public void submit() throws Exception {
    val command = new ProcessExecutor()
        .command(java(), "-cp", classpath(), mainClass())
        .redirectError(logOutputStream())
        .redirectOutput(logOutputStream())
        .readOutput(true);

    val result = command.execute();
    log.info(result.outputUTF8());
  }

  private String mainClass() {
    return WorkflowMain.class.getName();
  }

  private String classpath() {
    val separator = System.getProperty("path.separator");
    val classpath = System.getProperty("java.class.path");
    log.info(classpath);

    return isExpoded() ? getLocation(getClass()) + separator + classpath : getLocation(Job.class);
  }

  private boolean isExpoded() {
    return getLocation(getClass()).endsWith("classes");
  }

  private static String getLocation(Class<?> type) {
    return type.getProtectionDomain().getCodeSource().getLocation().getPath();
  }

  private static String java() {
    val separator = System.getProperty("file.separator");
    val javaHome = System.getProperty("java.home");
    log.info(javaHome);

    return javaHome + separator + "bin" + separator + "java";
  }

  private static Slf4jOutputStream logOutputStream() {
    return Slf4jStream.of(log).asInfo();
  }

}
