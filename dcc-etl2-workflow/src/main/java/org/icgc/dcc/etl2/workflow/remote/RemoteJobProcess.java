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
package org.icgc.dcc.etl2.workflow.remote;

import static com.google.common.collect.ImmutableList.of;

import java.net.ServerSocket;

import org.icgc.dcc.etl2.core.job.Job;
import org.slf4j.LoggerFactory;
import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.StartedProcess;
import org.zeroturnaround.exec.stream.slf4j.Slf4jOutputStream;
import org.zeroturnaround.exec.stream.slf4j.Slf4jStream;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
@ToString(exclude = "process")
public class RemoteJobProcess implements AutoCloseable {

  /**
   * Configuration.
   */
  private final Class<? extends Job> jobClass;
  @Getter
  private int port;

  /**
   * State.
   */
  private StartedProcess process;

  public void start() throws Exception {
    this.port = findFreePort();

    val command = new ProcessExecutor()
        .command(serverCommand(port))
        .redirectError(logOutputStream())
        .redirectOutput(logOutputStream())
        .destroyOnExit();

    this.process = command.start();
  }

  public void stop() throws Exception {
    close();
  }

  @Override
  public void close() throws Exception {
    process.getProcess().destroy();
  }

  private Iterable<String> serverCommand(int port) {
    val command = of(
        javaBinary(),
        "-Dcom.sun.management.jmxremote.port=" + port,
        "-Dcom.sun.management.jmxremote.local.only=true",
        "-Dcom.sun.management.jmxremote.authenticate=false",
        "-Dcom.sun.management.jmxremote.ssl=false",
        "-cp", classpath(),
        mainClass(),
        "--job.class=" + jobClass.getName());

    log.info("Command: {}", command);
    return command;
  }

  private String mainClass() {
    return RemoteJobMain.class.getName();
  }

  private String classpath() {
    val separator = System.getProperty("path.separator");
    val classpath = System.getProperty("java.class.path");

    return isExpoded() ? getLocation(getClass()) + separator + classpath : getLocation(jobClass);
  }

  private boolean isExpoded() {
    val separator = System.getProperty("file.separator");
    val location = getLocation(getClass());

    return location.endsWith("classes") || location.endsWith("classes" + separator);
  }

  private Slf4jOutputStream logOutputStream() {
    val name = "[" + jobClass.getSimpleName() + ":" + port + "]";
    val logger = LoggerFactory.getLogger(name);

    return Slf4jStream.of(logger).asInfo();
  }

  private static String getLocation(Class<?> type) {
    return type.getProtectionDomain().getCodeSource().getLocation().getPath();
  }

  private static String javaBinary() {
    val separator = System.getProperty("file.separator");
    val javaHome = System.getProperty("java.home");
    log.info("java.home: {}", javaHome);

    return javaHome + separator + "bin" + separator + "java";
  }

  @SneakyThrows
  private static int findFreePort() {
    try (val socket = new ServerSocket(0)) {
      socket.setReuseAddress(true);
      return socket.getLocalPort();
    }
  }

}
