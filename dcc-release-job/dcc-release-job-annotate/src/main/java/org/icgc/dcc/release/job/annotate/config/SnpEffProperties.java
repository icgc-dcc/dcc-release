package org.icgc.dcc.release.job.annotate.config;

import java.io.File;
import java.io.Serializable;

import lombok.Data;

/**
 * SnpEff specific properties.<br>
 * The class is declared as a {@code @Component} to avoid serialization errors.
 * @see https://github.com/spring-projects/spring-boot/issues/1811
 */
@Data
public class SnpEffProperties implements Serializable {

  /**
   * Configuration.
   */
  private File resourceDir;
  private String resourceUrl;
  private String version;
  private String databaseVersion;
  private String referenceGenomeVersion;
  private String geneBuildVersion;
  private int maxFileSizeMb;

}
