package org.icgc.dcc.release.job.export.function;

import static org.icgc.dcc.release.job.export.model.type.Constants.*;
import lombok.val;

import org.apache.spark.api.java.function.Function;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class IsOpenControlled implements Function<ObjectNode, Boolean> {

  @Override
  public Boolean call(ObjectNode row) {
    val markingValue = row.get(MARKING_FIELD_NAME);

    return !(markingValue == null || markingValue.isNull() || markingValue.isMissingNode())
        && (markingValue.asText().equals(OPEN_FIELD_NAME) || markingValue.asText().equals(CONTROLLED_FIELD_NAME));
  }
}
