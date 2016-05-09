package org.icgc.dcc.release.job.index.config;

import static com.google.common.collect.Lists.newArrayList;

import java.util.List;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
@NoArgsConstructor
public class IndexProperties {

  /**
   * The output ES URI.
   */
  @NonNull
  String esUri;

  /**
   * DocumentTypes to load.
   */
  List<String> includeTypes = newArrayList();

  /**
   * DocumentTypes to exclude.
   */
  List<String> excludeTypes = newArrayList();

  /**
   * Threshold for big documents filtering.
   */
  int bigDocumentThresholdMb = 100;

  /**
   * Load only big documents.
   */
  boolean bigDocumentsOnly = false;

}
