package org.icgc.dcc.etl2.core.submission;

import java.io.Serializable;
import java.util.Map;

import lombok.NonNull;
import lombok.ToString;
import lombok.val;

import com.google.common.collect.ImmutableMap;

@ToString
public class Schemas implements Serializable {

  /**
   * State
   */
  private final Map<String, Schema> values;

  public Schemas(@NonNull Iterable<Schema> values) {
    val map = ImmutableMap.<String, Schema> builder();
    for (val schema : values) {
      map.put(schema.getName(), schema);
    }

    this.values = map.build();
  }

  public Schema get(@NonNull String schemaName) {
    return values.get(schemaName);
  }

}
