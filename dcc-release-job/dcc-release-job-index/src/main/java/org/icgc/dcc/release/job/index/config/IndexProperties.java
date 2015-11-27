package org.icgc.dcc.release.job.index.config;

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
	   * The output archive dir.
	   */
	  @NonNull
	  String outputDir;

}
