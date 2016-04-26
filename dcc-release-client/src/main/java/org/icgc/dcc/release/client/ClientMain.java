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
package org.icgc.dcc.release.client;

import static com.google.common.base.Strings.repeat;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.lang.System.err;
import static org.icgc.dcc.release.client.util.Names.APPLICATION_BASE_NAME;

import java.util.concurrent.TimeUnit;

import org.icgc.dcc.release.client.cli.Options;
import org.icgc.dcc.release.client.core.Workflow;
import org.icgc.dcc.release.client.core.WorkflowContext;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
@EnableAutoConfiguration
@ComponentScan("org.icgc.dcc.release")
public class ClientMain {

  /**
   * Constants.
   */
  public static final String APPLICATION_NAME = APPLICATION_BASE_NAME + "-client";
  public static final int SUCCESS_STATUS_CODE = 0;
  public static final int FAILURE_STATUS_CODE = 1;

  public static void main(String[] args) {
    val options = new Options();
    val cli = new JCommander(options);
    cli.setAcceptUnknownOptions(true);
    cli.setProgramName(APPLICATION_NAME);

    int exitCode = SUCCESS_STATUS_CODE;
    try {
      cli.parse(args);

      banner("Running with {}", options);
      execute(options, args);
    } catch (ParameterException e) {
      log.error("Invalid parameter(s): ", e);
      err.println("Invalid parameter(s): " + e.getMessage());
      usage(cli);

      exitCode = FAILURE_STATUS_CODE;
    } catch (Exception e) {
      log.error("Unknown error: ", e);
      err.println("Unknow error. Please check the log for detailed error messages: " + e.getMessage());

      exitCode = FAILURE_STATUS_CODE;
    } finally {
      exit(exitCode);
    }
  }

  private static void execute(Options options, String[] args) {
    val applicationContext = new SpringApplicationBuilder(ClientMain.class).web(false).run(args);
    val workflow = applicationContext.getBean(Workflow.class);
    val workflowContext = createWorkflowContext(options);
    log.info("{}\n", repeat("-", 100));

    workflow.execute(workflowContext);
  }

  private static WorkflowContext createWorkflowContext(Options options) {
    return new WorkflowContext(
        options.release,
        options.projectNames,
        options.releaseDir,
        options.stagingDir,
        options.jobs,
        options.compressOutput);
  }

  private static void usage(JCommander cli) {
    val message = new StringBuilder();
    cli.usage(message);
    err.println(message.toString());
  }

  private static void exit(int exitCode) {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      sleepUninterruptibly(5L, TimeUnit.SECONDS);

      log.info("\n");
      banner("Exiting with return code: {}", exitCode);
    }));

    System.exit(exitCode);
  }

  private static void banner(String message, Object... args) {
    log.info("{}", repeat("-", 100));
    log.info(message, args);
    log.info("{}", repeat("-", 100));
  }

}
