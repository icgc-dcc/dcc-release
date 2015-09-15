package org.icgc.dcc.release.core.submission;

import java.io.Serializable;
import java.util.Map;

import lombok.NonNull;
import lombok.Value;

import org.icgc.dcc.common.core.model.ValueType;

@Value
public class SubmissionFileField implements Serializable {

  @NonNull
  String name;
  @NonNull
  ValueType type;

  Map<String, String> terms;

}
