package org.icgc.dcc.etl2.core.submission;

import java.io.Serializable;
import java.util.List;

import lombok.Value;

@Value
public class Schema implements Serializable {

  String name;
  String pattern;
  List<Field> fields;

}
