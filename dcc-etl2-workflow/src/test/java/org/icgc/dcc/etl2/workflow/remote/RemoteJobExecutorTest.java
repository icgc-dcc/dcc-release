package org.icgc.dcc.etl2.workflow.remote;

import org.icgc.dcc.etl2.job.index.core.IndexJob;
import org.icgc.dcc.etl2.job.stage.core.StageJob;
import org.icgc.dcc.etl2.workflow.remote.RemoteJobExecutor;
import org.junit.Test;

import lombok.val;

public class RemoteJobExecutorTest {

  @Test
  public void testSubmit() throws Exception {
    val executor = new RemoteJobExecutor();

    executor.submit(StageJob.class);
    executor.submit(IndexJob.class);
  }

}
