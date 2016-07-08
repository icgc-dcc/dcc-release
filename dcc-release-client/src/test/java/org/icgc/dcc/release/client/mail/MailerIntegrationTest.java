/*
 * Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.release.client.mail;

import static com.google.common.base.Stopwatch.createStarted;
import lombok.val;

import org.icgc.dcc.release.client.config.MailConfig;
import org.icgc.dcc.release.client.config.WorkflowProperties;
import org.icgc.dcc.release.client.config.WorkflowProperties.MailProperties;
import org.icgc.dcc.release.client.mail.MailerIntegrationTest.TestMailConfig;
import org.icgc.dcc.release.core.job.JobSummary;
import org.icgc.dcc.release.core.job.JobType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.thymeleaf.TemplateEngine;

@ActiveProfiles("development")
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = { TestMailConfig.class })
public class MailerIntegrationTest {

  /**
   * Dependencies.
   */
  @Autowired
  JavaMailSender mailSender;
  @Autowired
  TemplateEngine templateEngine;
  @Autowired
  MailProperties mail;

  @Test
  public void testSendJobFinished() {
    val mailer = new Mailer(mailSender, templateEngine, mail);

    val summary = new JobSummary(JobType.STAGE, createStarted());
    mailer.sendJobSummary(summary);
  }

  @Configuration
  @EnableAutoConfiguration
  @Import({ WorkflowProperties.class, MailConfig.class })
  public static class TestMailConfig {}

}
