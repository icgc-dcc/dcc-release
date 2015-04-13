package org.icgc.dcc.etl2.job.export.function;

import static org.icgc.dcc.etl2.job.export.model.Constants.MASKED_FIELD_VALUE;
import static org.icgc.dcc.etl2.job.export.model.Constants.OBSERVATION_FIELD_NAME;
import static org.icgc.dcc.etl2.job.export.model.Constants.OPEN_FIELD_VALUE;
import lombok.val;

import org.apache.spark.api.java.function.Function;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class isOpenMasked implements Function<ObjectNode, Boolean> {

  @Override
  public Boolean call(ObjectNode row) {
    val observationValue = row.get(OBSERVATION_FIELD_NAME);
    if (observationValue.isNull() || observationValue.isMissingNode()) {
      return false;
    }
    val markingValue = observationValue.get(OBSERVATION_FIELD_NAME);
    if (markingValue.isNull() || markingValue.isMissingNode()) {
      return false;
    }

    return markingValue.asText().equals(OPEN_FIELD_VALUE) || markingValue.asText().equals(MASKED_FIELD_VALUE);
  }
}
