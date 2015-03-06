package org.icgc.dcc.etl2.core.submission;

import java.io.Serializable;
import java.util.Map;

import lombok.Value;

import org.icgc.dcc.common.core.model.ValueType;

@Value
public class Field implements Serializable {

  String name;
  ValueType type;
  Map<String, String> terms;

}
