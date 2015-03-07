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
package org.icgc.dcc.etl2.workflow;

import static com.google.common.base.Strings.repeat;
import static java.lang.System.err;
import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.etl2.workflow.cli.Options;
import org.icgc.dcc.etl2.workflow.core.Workflow;
import org.icgc.dcc.etl2.workflow.core.WorkflowContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

@Slf4j
@Configuration
@EnableAutoConfiguration
@ComponentScan("org.icgc.dcc.etl2")
public class WorkflowMain implements CommandLineRunner, ExitCodeGenerator {

  /**
   * Constants.
   */
  public static final String APPLICATION_NAME = "dcc-etl2-workflow";
  public static final int SUCCESS_STATUS_CODE = 0;
  public static final int FAILURE_STATUS_CODE = 1;

  /**
   * Dependencies.
   */
  @Autowired
  private Workflow workflow;

  /**
   * State.
   */
  @Getter
  private int exitCode;

  public static void main(String[] args) {
    SpringApplication.run(WorkflowMain.class, args);
  }

  @Override
  public void run(String... args) throws Exception {
    val options = new Options();
    val cli = new JCommander(options);
    cli.setAcceptUnknownOptions(true);
    cli.setProgramName(APPLICATION_NAME);

    try {
      cli.parse(args);
      log.info("{}", repeat("-", 100));
      log.info("Running with {}", options);
      log.info("{}\n", repeat("-", 100));

      execute(options);

      exitCode = SUCCESS_STATUS_CODE;
    } catch (ParameterException e) {
      err.println("Invalid parameter(s): " + e.getMessage());
      usage(cli);

      exitCode = FAILURE_STATUS_CODE;
    } catch (Exception e) {
      log.error("Unknown error: ", e);
      err.println("Command error. Please check the log for detailed error messages: " + e.getMessage());

      exitCode = FAILURE_STATUS_CODE;
    }
  }

  private void execute(Options options) {
    val workflowContext = createWorkflowContext(options);

    workflow.execute(workflowContext);
  }

  private static WorkflowContext createWorkflowContext(Options options) {
    // TODO: Derive from options
    return new WorkflowContext(
        Temp.RELEASE_NAME,
        Temp.PROJECT_NAMES,
        Temp.RELEASE_DIR,
        Temp.STAGING_DIR,

        options.jobs);
  }

  private static void usage(JCommander cli) {
    val message = new StringBuilder();
    cli.usage(message);
    err.println(message.toString());
  }

}
