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
package org.icgc.dcc.release.workflow.mail;

import static com.google.common.base.CaseFormat.LOWER_HYPHEN;
import static com.google.common.base.CaseFormat.UPPER_CAMEL;
import static com.google.common.collect.ImmutableMap.of;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.springframework.util.MimeTypeUtils.IMAGE_PNG_VALUE;

import java.util.Map;

import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.release.core.job.JobSummary;
import org.icgc.dcc.release.workflow.config.WorkflowProperties.MailProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Service;
import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.Context;

/**
 * See http://www.thymeleaf.org/doc/articles/springmail.html
 */
@Slf4j
@Service
@RequiredArgsConstructor(onConstructor = @__({ @Autowired }))
public class Mailer {

  /**
   * Constants.
   */
  private static final String SUBJECT_PREFIX = "DCC Workflow - ";
  private static final String JOB_SUMMARY_TEMPLATE_NAME = "job-summary";
  private static final Resource ICGC_LOGO = new ClassPathResource("/templates/icgc-logo-no-text.png");

  /**
   * Dependencies.
   */
  @NonNull
  private final JavaMailSender mailSender;
  @NonNull
  private final TemplateEngine templateEngine;
  @NonNull
  private final MailProperties mail;

  public void sendJobSummary(@NonNull JobSummary summary) {
    send(JOB_SUMMARY_TEMPLATE_NAME, of("summary", summary));
  }

  @SneakyThrows
  private void send(String templateName, Map<String, ?> variables) {
    val body = createBody(templateName, variables);
    val subject = createSubject(templateName);
    val mimeMessage = createMimeMessage(subject, body);

    log.info("Sending...");
    mailSender.send(mimeMessage);
    log.info("Sent: '{}'", mimeMessage.getSubject());
  }

  private String createSubject(String templateName) {
    val jobDescription = LOWER_HYPHEN.to(UPPER_CAMEL, templateName).replaceAll(
        String.format("%s|%s|%s",
            "(?<=[A-Z])(?=[A-Z][a-z])",
            "(?<=[^A-Z])(?=[A-Z])",
            "(?<=[A-Za-z])(?=[^A-Za-z])"
            ),
        " "
        );

    return SUBJECT_PREFIX + jobDescription;
  }

  private String createBody(String templateName, Map<String, ?> variables) {
    val context = new Context();
    context.setVariables(variables);

    return templateEngine.process(templateName, context);
  }

  private MimeMessage createMimeMessage(String subject, String text) throws MessagingException {
    val message = new MimeMessageHelper(mailSender.createMimeMessage(), true, UTF_8.name());
    message.setSubject(subject);
    message.setText(text, true);
    message.setTo(mail.getRecipients());
    message.addInline("logo", ICGC_LOGO, IMAGE_PNG_VALUE);

    return message.getMimeMessage();
  }

}
