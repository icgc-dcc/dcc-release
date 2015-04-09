package org.icgc.dcc.etl2.job.export.function;

import lombok.NonNull;

import org.apache.spark.api.java.function.Function;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class isType implements Function<ObjectNode, Boolean> {

  @NonNull
  private final String type;

  public isType(String type) {
    this.type = type;
  }

  @Override
  public Boolean call(ObjectNode row) {
    return row.get("_type").asText().equalsIgnoreCase(type);
  }
}
