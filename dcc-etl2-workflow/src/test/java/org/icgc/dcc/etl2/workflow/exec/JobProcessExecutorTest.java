package org.icgc.dcc.etl2.workflow.exec;

import org.junit.Test;

import lombok.val;

public class JobProcessExecutorTest {

  @Test
  public void testSubmit() throws Exception {
    val executor = new JobProcessExecutor();
    executor.submit();
  }

}
