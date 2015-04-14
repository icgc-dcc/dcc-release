package org.icgc.dcc.etl2.job.export.function;

import static org.icgc.dcc.etl2.job.export.model.Constants.CONTROLLED_FIELD_VALUE;
import static org.icgc.dcc.etl2.job.export.model.Constants.MARKING_FIELD_VALUE;
import static org.icgc.dcc.etl2.job.export.model.Constants.OPEN_FIELD_VALUE;
import lombok.val;

import org.apache.spark.api.java.function.Function;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class isOpenControlled implements Function<ObjectNode, Boolean> {

  @Override
  public Boolean call(ObjectNode row) {
    val markingValue = row.get(MARKING_FIELD_VALUE);
    if (markingValue == null || markingValue.isNull() || markingValue.isMissingNode()) {
      return false;
    }

    return markingValue.asText().equals(OPEN_FIELD_VALUE) || markingValue.asText().equals(CONTROLLED_FIELD_VALUE);
  }
}
