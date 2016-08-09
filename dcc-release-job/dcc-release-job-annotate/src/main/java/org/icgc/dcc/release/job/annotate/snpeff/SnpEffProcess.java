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
package org.icgc.dcc.release.job.annotate.snpeff;

import static com.google.common.io.Resources.getResource;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.File;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

import com.google.common.io.Files;
import com.google.common.io.Resources;

/**
 * Abstraction that manages the creating of the forked SnpEff process.
 */
@Slf4j
public class SnpEffProcess extends Process {

  /**
   * Configuration.
   */
  private final File jar;
  private final File java;
  private final File dataDir;
  private final String databaseVersion;

  /**
   * State.
   */
  @Delegate
  private final Process delegate;

  public SnpEffProcess(@NonNull File jar, @NonNull File java, @NonNull File dataDir, @NonNull String databaseVersion) {
    this.jar = jar;
    this.java = java;
    this.dataDir = dataDir;
    this.databaseVersion = databaseVersion;
    this.delegate = createProcess();
  }

  @SneakyThrows
  private Process createProcess() {
    val builder = new ProcessBuilder(
        getJavaPath(),
        "-Xmx4g",
        "-cp",
        getClassPath(),
        getMainClass(),
        "eff",
        "-cancer",
        "-v",
        "-geneId",
        "-sequenceOntology",
        "-noStats",
        "-noLog",
        "-c",
        getConfigFile(),
        databaseVersion);

    log.error("Process command: {}", builder.command());
    return builder.start();
  }

  @SneakyThrows
  private String getConfigFile() {
    // Template
    val contents = Resources.toString(getResource("snpEff.config"), UTF_8)
        .replace("${dataDir}", dataDir.getAbsolutePath());

    val configFile = File.createTempFile("snpEff", ".config");

    log.info("Creating temporary configuration file at '{}'...", configFile);
    Files.write(contents, configFile, UTF_8);

    return configFile.getAbsolutePath();
  }

  private String getJavaPath() {
    return java.getAbsolutePath();
  }

  private String getMainClass() {
    // This cannot use SnpEff.class.getName() since we are not running Java 7 yet.
    // TODO: Use SnpEff.class.getName()
    return "ca.mcgill.mcb.pcingola.snpEffect.commandLine.SnpEff";
  }

  private String getClassPath() {
    return jar.getAbsolutePath();
  }

}
