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
package org.icgc.dcc.etl2.workflow.mail;

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

import org.icgc.dcc.etl2.core.job.JobSummary;
import org.icgc.dcc.etl2.workflow.config.WorkflowProperties.MailProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
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
  private static final String JOB_SUMMARY_TEMPLATE_NAME = "job-summary";
  private static final ClassPathResource ICGC_LOGO = new ClassPathResource("/templates/icgc-logo-no-text.png");

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
    val context = createContext(variables);
    val text = renderText(templateName, context);
    val mimeMessage = createMimeMessage(templateName, text);

    send(mimeMessage);
  }

  private String renderText(String templateName, Context context) {
    val text = templateEngine.process(templateName, context);

    return text;
  }

  @SneakyThrows
  private void send(MimeMessage mimeMessage) {
    log.info("Sending...");
    mailSender.send(mimeMessage);
    log.info("Sent: '{}'", mimeMessage.getSubject());
  }

  private Context createContext(Map<String, ?> variables) {
    val context = new Context();
    context.setVariables(variables);

    return context;
  }

  private MimeMessage createMimeMessage(String templateName, String text) throws MessagingException {
    val message = new MimeMessageHelper(mailSender.createMimeMessage(), true, UTF_8.name());
    message.setSubject("DCC Workflow - " + templateName);
    message.setText(text, true);
    message.setTo(mail.getRecipients());
    message.addInline("logo", ICGC_LOGO, IMAGE_PNG_VALUE);

    return message.getMimeMessage();
  }

}
