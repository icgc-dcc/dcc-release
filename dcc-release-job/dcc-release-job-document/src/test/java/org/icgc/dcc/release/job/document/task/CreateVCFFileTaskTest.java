package org.icgc.dcc.release.job.document.task;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.release.job.index.utils.TestUtils.createSnpEffProperties;

import java.io.File;

import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.core.io.Files2;
import org.icgc.dcc.release.core.job.JobType;
import org.icgc.dcc.release.core.task.Task;
import org.icgc.dcc.release.test.job.AbstractJobTest;
import org.junit.Before;
import org.junit.Test;

@Slf4j
public class CreateVCFFileTaskTest extends AbstractJobTest {

  private static final String COMPRESSED_INPUT = TEST_FIXTURES_DIR + "/compressed/input";

  Task task = new CreateVCFFileTask(createSnpEffProperties());

  @Before
  @Override
  public void setUp() {
    super.setUp();
  }

  @Test
  public void testExecute() throws Exception {
    given(new File(INPUT_TEST_FIXTURES_DIR));
    task.execute(createTaskContext(JobType.SUMMARIZE));
    verifyOutput();
  }

  @Test
  public void testExecute_compressed() throws Exception {
    given(new File(COMPRESSED_INPUT));
    task.execute(createCompressedTaskContext(JobType.SUMMARIZE));
    verifyOutput();
  }

  private void verifyOutput() {
    val vcfFile = resolveVcfFile();
    printFile(vcfFile);
    assertThat(vcfFile.exists()).isTrue();
    assertThat(vcfFile.length()).isGreaterThan(1L);
  }

  @SneakyThrows
  private static void printFile(File vcfFile) {
    val reader = Files2.getCompressionAgnosticBufferedReader(vcfFile.getAbsolutePath());
    String line = null;
    while ((line = reader.readLine()) != null) {
      log.info(line);
    }
  }

  private File resolveVcfFile() {
    return new File(workingDir, CreateVCFFileTask.VCF_FILE_NAME);
  }

}
