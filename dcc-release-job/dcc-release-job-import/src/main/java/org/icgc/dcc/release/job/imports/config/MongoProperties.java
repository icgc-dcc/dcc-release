package org.icgc.dcc.release.job.imports.config;

import lombok.Data;

/**
 * MongoDB specific properties.
 */
@Data
public class MongoProperties {

  /**
   * Configuration.
   */
  private String uri = "mongodb://localhost:27017";
  private String userName;
  private String password;
  private int splitSizeMb = 8; // MB

}
