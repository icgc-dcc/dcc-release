package org.icgc.dcc.release.core.submission;

import java.io.Serializable;
import java.util.Map;

import lombok.NonNull;
import lombok.ToString;
import lombok.val;

import com.google.common.collect.ImmutableMap;

@ToString
public class SubmissionFileSchemas implements Serializable {

  /**
   * State
   */
  private final Map<String, SubmissionFileSchema> fileSchemas;

  public SubmissionFileSchemas(@NonNull Iterable<SubmissionFileSchema> values) {
    val map = ImmutableMap.<String, SubmissionFileSchema> builder();
    for (val schema : values) {
      map.put(schema.getName(), schema);
    }

    this.fileSchemas = map.build();
  }

  public SubmissionFileSchema get(@NonNull String schemaName) {
    return fileSchemas.get(schemaName);
  }

}
