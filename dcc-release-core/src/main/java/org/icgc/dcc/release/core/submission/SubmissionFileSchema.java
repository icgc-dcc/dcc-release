package org.icgc.dcc.release.core.submission;

import java.io.Serializable;
import java.util.List;

import lombok.Value;

@Value
public class SubmissionFileSchema implements Serializable {

  String name;
  String pattern;
  List<SubmissionFileField> fields;

}
