package org.icgc.dcc.release.job.index.task;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.release.job.index.utils.TestUtils.createSnpEffProperties;

import java.io.File;

import lombok.val;

import org.icgc.dcc.release.core.job.JobType;
import org.icgc.dcc.release.core.task.Task;
import org.icgc.dcc.release.test.job.AbstractJobTest;
import org.junit.Before;
import org.junit.Test;

public class CreateVCFFileTaskTest extends AbstractJobTest {

  Task task = new CreateVCFFileTask(createSnpEffProperties());

  @Before
  @Override
  public void setUp() {
    super.setUp();
    given(new File(TEST_FIXTURES_DIR));
  }

  @Test
  public void testExecute() throws Exception {
    task.execute(createTaskContext(JobType.SUMMARIZE));
    verifyOutput();
  }

  private void verifyOutput() {
    val vcfFile = resolveVcfFile();
    assertThat(vcfFile.exists()).isTrue();
    assertThat(vcfFile.length()).isGreaterThan(1L);
  }

  private File resolveVcfFile() {
    return new File(workingDir, CreateVCFFileTask.VCF_FILE_NAME);
  }

}
