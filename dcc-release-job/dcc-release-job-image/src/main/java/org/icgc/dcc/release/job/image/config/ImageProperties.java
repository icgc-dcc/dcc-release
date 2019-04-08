package org.icgc.dcc.release.job.image.config;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.io.Serializable;

@Data
@Accessors(chain = true)
@NoArgsConstructor
public class ImageProperties implements Serializable {

  /**
   * Whether to skip getting Urls
   */
  boolean skipUrls;

}
