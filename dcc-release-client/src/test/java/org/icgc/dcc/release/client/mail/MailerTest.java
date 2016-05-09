package org.icgc.dcc.release.client.mail;

import static com.google.common.base.Stopwatch.createStarted;
import lombok.val;

import org.icgc.dcc.release.client.config.MailConfig;
import org.icgc.dcc.release.client.config.WorkflowProperties;
import org.icgc.dcc.release.client.config.WorkflowProperties.MailProperties;
import org.icgc.dcc.release.client.mail.MailerTest.TestMailConfig;
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
public class MailerTest {

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
