package org.icgc.dcc.etl2.workflow.mail;

import static org.icgc.dcc.etl2.core.util.Stopwatches.createStarted;
import lombok.val;

import org.icgc.dcc.etl2.core.job.JobSummary;
import org.icgc.dcc.etl2.core.job.JobType;
import org.icgc.dcc.etl2.workflow.config.MailConfig;
import org.icgc.dcc.etl2.workflow.config.WorkflowProperties;
import org.icgc.dcc.etl2.workflow.config.WorkflowProperties.MailProperties;
import org.icgc.dcc.etl2.workflow.mail.MailerTest.TestMailConfig;
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
