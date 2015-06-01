package org.icgc.dcc.etl2.job.export.function;

import org.apache.spark.api.java.function.Function;

import com.fasterxml.jackson.databind.node.ObjectNode;

class All implements Function<ObjectNode, Boolean> {

  @Override
  public Boolean call(ObjectNode row) {
    return true;
  }
}
