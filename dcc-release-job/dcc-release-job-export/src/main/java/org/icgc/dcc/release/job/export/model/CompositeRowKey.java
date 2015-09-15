package org.icgc.dcc.release.job.export.model;

import lombok.AllArgsConstructor;
import lombok.Value;

@Value
@AllArgsConstructor
public class CompositeRowKey {
  int donorId;
  long index;
}
