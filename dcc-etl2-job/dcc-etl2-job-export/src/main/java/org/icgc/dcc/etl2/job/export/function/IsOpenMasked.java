package org.icgc.dcc.etl2.job.export.function;

import static org.icgc.dcc.etl2.job.export.model.type.Constants.*;

import lombok.val;

import org.apache.spark.api.java.function.Function;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class IsOpenMasked implements Function<ObjectNode, Boolean> {

  @Override
  public Boolean call(ObjectNode row) {
    val markingValue = row.get(MARKING_FIELD_VALUE);

    return !(markingValue == null || markingValue.isNull() || markingValue.isMissingNode())
        && (markingValue.asText().equals(OPEN_FIELD_VALUE) || markingValue.asText().equals(MASKED_FIELD_VALUE));
  }
}
