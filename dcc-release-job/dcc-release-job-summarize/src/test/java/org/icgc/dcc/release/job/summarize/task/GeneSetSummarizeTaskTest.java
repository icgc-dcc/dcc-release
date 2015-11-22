package org.icgc.dcc.release.job.summarize.task;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.core.model.FieldNames.LoaderFieldNames.SUMMARY;
import static org.icgc.dcc.release.core.util.ObjectNodes.textValue;

import java.io.File;

import lombok.val;

import org.icgc.dcc.release.core.job.FileType;
import org.icgc.dcc.release.core.job.JobType;
import org.icgc.dcc.release.test.job.AbstractJobTest;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class GeneSetSummarizeTaskTest extends AbstractJobTest {

  GeneSetSummarizeTask task;

  @Before
  @Override
  public void setUp() {
    super.setUp();
    task = new GeneSetSummarizeTask();
  }

  @Test
  public void testExecute() {
    given(new File(INPUT_TEST_FIXTURES_DIR));
    task.execute(createTaskContext(JobType.SUMMARIZE));

    val result = produces(FileType.GENE_SET_SUMMARY);
    assertThat(result).hasSize(2);
    for (val geneSet : result) {
      assertGeneSet(geneSet);
    }
  }

  private static void assertGeneSet(ObjectNode geneSet) {
    val id = textValue(geneSet, "id");
    val geneCount = geneSet.get(SUMMARY).get("_gene_count").asInt();
    if (id.equals("GS1")) {
      assertThat(geneCount).isEqualTo(2);
    } else {
      assertThat(geneCount).isEqualTo(1);
    }
  }

}
