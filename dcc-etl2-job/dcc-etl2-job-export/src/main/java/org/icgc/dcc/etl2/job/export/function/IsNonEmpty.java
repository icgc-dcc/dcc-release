package org.icgc.dcc.etl2.job.export.function;

import lombok.NonNull;
import lombok.val;

import org.apache.spark.api.java.function.Function;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class IsNonEmpty implements Function<ObjectNode, Boolean> {

  @NonNull
  private final String fieldName;

  public IsNonEmpty(String fieldName) {
    this.fieldName = fieldName;
  }

  @Override
  public Boolean call(ObjectNode row) {
    val value = row.get(fieldName);
    return !(value == null || value.isNull() || value.isMissingNode());
  }
}
